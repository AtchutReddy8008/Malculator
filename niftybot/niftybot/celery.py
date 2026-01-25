import os
from celery import Celery
from django.conf import settings

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'niftybot.settings')

app = Celery('niftybot')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks(['trading'])

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')