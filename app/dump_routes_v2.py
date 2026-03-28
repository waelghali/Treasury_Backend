import sys
sys.path.append('c:\\Grow')
from app.main import app, configure_app_instance
configure_app_instance(app)

with open('c:\\Grow\\app\\routes.txt', 'w') as f:
    for route in app.routes:
        if hasattr(route, 'methods'):
            f.write(f"{list(route.methods)[0]} {route.path}\n")
