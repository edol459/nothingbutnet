web: gunicorn --workers 2 --threads 8 --worker-class gthread --timeout 60 --max-requests 1000 --max-requests-jitter 100 server:app
