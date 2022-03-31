web: uwsgi --chdir django --module wsgi --master --socket 127.0.0.1:8000
postdeploy: python manage.py migrate