import virtualenv
import os


def prepare_venv():
    venv_dir = os.path.join(os.getcwd(), ".test-venv")
    virtualenv.create_environment(venv_dir)
