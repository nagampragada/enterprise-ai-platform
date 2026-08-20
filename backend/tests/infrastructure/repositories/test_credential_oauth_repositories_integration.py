from datetime import datetime,timedelta,timezone
import os,subprocess,threading,uuid
from pathlib import Path
import pytest
from sqlalchemy import create_engine,func,select,text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session,sessionmaker
from application.ports.github_app import GitHubInstallation
from infrastructure.db.models import ConnectorCredential,OAuthAuthorizationTransaction
from infrastructure.repositories.connector_credential_repository import ConnectorCredentialRepository
from infrastructure.repositories.github_app_installation_repository import GitHubAppInstallationRepository,GitHubInstallationConflict
from infrastructure.repositories.oauth_authorization_transaction_repository import OAuthAuthorizationTransactionRepository,OAuthTransactionConflict

ROOT=Path(__file__).resolve().parents[3];PYTHON=ROOT/".venv"/"Scripts"/"python.exe";INI=ROOT/"alembic.ini"
NOW=datetime(2026,8,25,12,tzinfo=timezone.utc)
def _identity(url):v=make_url(url);return v.drivername,v.host,v.port,v.database
@pytest.fixture(scope="module")
def engine():
    url=os.environ["TEST_DATABASE_URL"];dev=os.environ.get("DATABASE_URL")
    if dev and _identity(dev)==_identity(url):raise RuntimeError("test database must differ from development database")
    reset=create_engine(url,future=True)
    with reset.begin() as c:c.execute(text("DROP SCHEMA IF EXISTS public CASCADE"));c.execute(text("CREATE SCHEMA public"))
    reset.dispose();env=os.environ.copy();env["DATABASE_URL"]=url
    subprocess.run([str(PYTHON),"-m","alembic","-c",str(INI),"upgrade","head"],check=True,cwd=str(ROOT),env=env)
    value=create_engine(url,future=True);yield value;value.dispose()
@pytest.fixture(autouse=True)
def clean(engine):
    with engine.begin() as c:
        for table in ("oauth_authorization_transactions","connector_credentials","connectors","users","organization_settings","organizations"):
            c.execute(text(f"DELETE FROM {table}"))
@pytest.fixture
def factory(engine):return sessionmaker(bind=engine,class_=Session,expire_on_commit=False)
def _setup(factory,name):
    org,user,connector=uuid.uuid4(),uuid.uuid4(),uuid.uuid4();s=factory()
    s.execute(text("INSERT INTO organizations(id,name,slug) VALUES(:id,:name,:slug)"),{"id":org,"name":name,"slug":f"{name}-{org}"})
    s.execute(text("""INSERT INTO users(id,organization_id,email,normalized_email,password_hash,display_name)
        VALUES(:id,:org,:email,:email,'hash',:name)"""),{"id":user,"org":org,"email":f"{user}@x.test","name":name})
    s.execute(text("""INSERT INTO connectors(id,organization_id,connector_type,display_name,slug,status)
        VALUES(:id,:org,'github',:name,:slug,'active')"""),{"id":connector,"org":org,"name":name,"slug":str(connector)})
    s.commit();s.close();return org,user,connector
def test_credential_replace_is_single_tenant_safe_and_rollback_owned(factory):
    org,user,connector=_setup(factory,"one");other,_,_=_setup(factory,"two");s=factory();repo=ConnectorCredentialRepository(s)
    first=repo.replace(org,connector,provider_key="github",auth_scheme="oauth2",secret_reference="ref-one",external_subject=None,display_label=None,granted_scopes=("repo",),expires_at=None,created_by_user_id=user,now=NOW)
    s.commit();second=repo.replace(org,connector,provider_key="github",auth_scheme="oauth2",secret_reference="ref-two",external_subject="acct",display_label="Account",granted_scopes=("repo",),expires_at=None,created_by_user_id=user,now=NOW+timedelta(seconds=1))
    assert second.metadata.credential_id==first.metadata.credential_id and second.previous_secret_reference=="ref-one"
    s.rollback();assert repo.get(org,connector).external_subject is None and repo.get(other,connector) is None
    assert s.scalar(select(func.count()).select_from(ConnectorCredential))==1;s.close()
def test_oauth_concurrent_consumption_has_one_winner_and_expiration_is_bounded(factory):
    org,user,connector=_setup(factory,"oauth");setup=factory();repo=OAuthAuthorizationTransactionRepository(setup)
    row=repo.create(org,connector,user,provider_key="github",state_hash=b"a"*32,pkce_reference="opaque",callback_identifier="github_callback",created_at=NOW,expires_at=NOW+timedelta(minutes=10));setup.commit();setup.close()
    barrier=threading.Barrier(2);outcomes=[]
    def consume():
        s=factory()
        try:
            barrier.wait();r=OAuthAuthorizationTransactionRepository(s).lock_by_state_hash(b"a"*32)
            if r is None or r.status!="pending":raise OAuthTransactionConflict("OAuth authorization transaction is unavailable")
            OAuthAuthorizationTransactionRepository(s).consume(r,now=NOW+timedelta(minutes=1));s.commit();outcomes.append("consumed")
        except OAuthTransactionConflict:s.rollback();outcomes.append("rejected")
        finally:s.close()
    threads=[threading.Thread(target=consume) for _ in range(2)]
    [t.start() for t in threads];[t.join() for t in threads];assert sorted(outcomes)==["consumed","rejected"]
    s=factory();repo=OAuthAuthorizationTransactionRepository(s)
    expired=repo.create(org,connector,user,provider_key="github",state_hash=b"b"*32,pkce_reference=None,callback_identifier="github_callback",created_at=NOW,expires_at=NOW+timedelta(minutes=1));s.commit()
    with pytest.raises(OAuthTransactionConflict):repo.consume(repo.lock_by_state_hash(b"b"*32),now=NOW+timedelta(minutes=2))
    assert repo.expire_stale(now=NOW+timedelta(minutes=2),limit=1)==1;s.commit()
    assert s.get(OAuthAuthorizationTransaction,expired.id).status=="expired";s.close()
def test_concurrent_cross_tenant_installation_binding_has_one_winner(factory):
    first_org,first_user,first_connector=_setup(factory,"first")
    second_org,second_user,second_connector=_setup(factory,"second")
    credentials=[]
    for org,user,connector in ((first_org,first_user,first_connector),(second_org,second_user,second_connector)):
        s=factory();value=ConnectorCredentialRepository(s).replace(org,connector,provider_key="github",
            auth_scheme="app_installation",secret_reference=None,external_subject="77",display_label="Org",
            granted_scopes=("metadata:read",),expires_at=None,created_by_user_id=user,now=NOW)
        s.commit();credentials.append(value.metadata.credential_id);s.close()
    candidate=GitHubInstallation(77,123,99,"org","Organization","selected",(("contents","read"),("metadata","read")),NOW,NOW)
    barrier=threading.Barrier(2);outcomes=[]
    def bind(org,connector,credential):
        s=factory()
        try:
            barrier.wait();GitHubAppInstallationRepository(s).bind(org,connector,credential,candidate,now=NOW)
            s.commit();outcomes.append("bound")
        except GitHubInstallationConflict:s.rollback();outcomes.append("rejected")
        finally:s.close()
    threads=[threading.Thread(target=bind,args=args) for args in ((first_org,first_connector,credentials[0]),(second_org,second_connector,credentials[1]))]
    [thread.start() for thread in threads];[thread.join() for thread in threads]
    assert sorted(outcomes)==["bound","rejected"]
def test_repository_errors_do_not_echo_sensitive_inputs(factory):
    org,user,connector=_setup(factory,"safe");s=factory();repo=OAuthAuthorizationTransactionRepository(s)
    repo.create(org,connector,user,provider_key="github",state_hash=b"z"*32,pkce_reference="secret-ref",callback_identifier="github_callback",created_at=NOW,expires_at=NOW+timedelta(minutes=1));s.commit()
    with pytest.raises(Exception) as caught:repo.create(org,connector,user,provider_key="github",state_hash=b"z"*32,pkce_reference="other-ref",callback_identifier="github_callback",created_at=NOW,expires_at=NOW+timedelta(minutes=1))
    assert all(value not in str(caught.value) for value in ("secret-ref","other-ref",(b"z"*32).hex(),"INSERT"));s.rollback();s.close()
