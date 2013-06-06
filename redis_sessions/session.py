import redis
import json
try:
    from django.utils.encoding import force_unicode
except ImportError:  # Python 3.*
    from django.utils.encoding import force_text as force_unicode
from django.contrib.sessions.backends.base import SessionBase, CreateError
from redis_sessions import settings
import pickle, base64

from django.contrib.sessions.backends.base import SessionBase, CreateError
from django.core.exceptions import SuspiciousOperation
from django.db import IntegrityError, transaction, router
from django.utils import timezone



# Avoid new redis connection on each request


if settings.SESSION_REDIS_URL is not None:
    redis_server = redis.StrictRedis.from_url(settings.SESSION_REDIS_URL)
elif settings.SESSION_REDIS_UNIX_DOMAIN_SOCKET_PATH is None:
    
    redis_server = redis.StrictRedis(
        host=settings.SESSION_REDIS_HOST,
        port=settings.SESSION_REDIS_PORT,
        db=settings.SESSION_REDIS_DB,
        password=settings.SESSION_REDIS_PASSWORD
    )
else:

    redis_server = redis.StrictRedis(
        unix_socket_path=settings.SESSION_REDIS_UNIX_DOMAIN_SOCKET_PATH,
        db=settings.SESSION_REDIS_DB,
        password=settings.SESSION_REDIS_PASSWORD,
    )


class SessionStore(SessionBase):
    """
    Implements Redis database session store.
    """
    def __init__(self, session_key=None):
        super(SessionStore, self).__init__(session_key)

        self.server = redis_server

    def load(self):
        try:
            s = Session.objects.get(
                session_key = self.session_key,
                expire_date__gt=timezone.now()
            )
            return self.decode(force_unicode(s.session_data))
        except (Session.DoesNotExist, SuspiciousOperation):
            self.create()
            return {}
        
    def exists(self, session_key):
        return Session.objects.filter(session_key=session_key).exists()
        

    def create(self):
        while True:
            self._session_key = self._get_new_session_key()
            try:
                # Save immediately to ensure we have a unique entry in the
                # database.
                self.save(must_create=True)
            except CreateError:
                # Key wasn't unique. Try again.
                continue
            self.modified = True
            self._session_cache = {}
            return

    def save(self, must_create=False):

        obj = Session(
            session_key=self._get_or_create_session_key(),
            session_data=self.encode(self._get_session(no_load=must_create)),
            expire_date=self.get_expiry_date()
        )
        using = router.db_for_write(Session, instance=obj)
        sid = transaction.savepoint(using=using)
        try:
            obj.save(force_insert=must_create, using=using)
        except IntegrityError:
            if must_create:
                transaction.savepoint_rollback(sid, using=using)
                raise CreateError
            raise

        data = self.encode(self._get_session(no_load=must_create))
        if redis.VERSION[0] >= 2:
            self.server.setex(
                self.get_real_stored_key(self._get_or_create_session_key()),
                self.get_expiry_age(),
                json.dumps(pickle.loads(base64.decodestring(data).split(":",1)[1]))
            )
            
        else:
            self.server.set(
                self.get_real_stored_key(self._get_or_create_session_key()),
                json.dumps(pickle.loads(base64.decodestring(data).split(":",1)[1]))
            )
            self.server.expire(
                self.get_real_stored_key(self._get_or_create_session_key()),
                self.get_expiry_age()
            )

    def delete(self, session_key=None):
        if session_key is None:
            if self.session_key is None:
                return
            session_key = self.session_key
        try:
            Session.objects.get(session_key=session_key).delete()
            self.server.delete(self.get_real_stored_key(session_key))
        except Session.DoesNotExist:
            pass

    def get_real_stored_key(self, session_key):
        """Return the real key name in redis storage
        @return string
        """
        prefix = settings.SESSION_REDIS_PREFIX
        if not prefix:
            return session_key
        return ':'.join([prefix, session_key])

# At bottom to avoid circular import
from django.contrib.sessions.models import Session