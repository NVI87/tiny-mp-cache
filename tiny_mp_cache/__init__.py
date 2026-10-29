from .tiny_mp_cache import TinyCache, serve

__all__ = ["TinyCache", "serve"]

# serve_unix собирается только на Unix-платформах (на Windows его нет)
try:
    from .tiny_mp_cache import serve_unix
except ImportError:
    pass
else:
    __all__.append("serve_unix")
