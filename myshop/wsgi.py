import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'myshop.settings')

# Standard Django application
application = get_wsgi_application()

# Vercel requires 'app' or 'handler' variable
app = application
