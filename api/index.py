import sys
import os

# Add parent directory to path so we can import main
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from main import app
from mangum import Mangum

# Vercel handler with Mangum adapter
handler = Mangum(app, lifespan="off")
