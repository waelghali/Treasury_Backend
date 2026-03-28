import sys
sys.path.append('c:\\Grow')
from app.main import app, configure_app_instance
configure_app_instance(app)

with open('c:\\Grow\\routes.txt', 'w') as f:
    for route in app.routes:
        if hasattr(route, 'methods'):
            f.write(f"{route.methods} {route.path}\n")
