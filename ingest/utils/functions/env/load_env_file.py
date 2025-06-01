import os

def load_env_file(
    env_path: str
):
    """
    Arguments:
    - env_path: File path to environment variables

    Load environment variables
    """

    print(f"[DEBUG] Attempting to load .env from : {env_path}")
    
    if os.path.exists(env_path):
        print(f"[DEBUG] .env file found")
        with open(env_path) as f:
            for line in f:
                # skip if line is not proper
                if line.strip() == "" or line.startswith('#'):
                    continue
                key_value = line.strip().split('=', 1)
                if len(key_value) == 2:
                    key, value = key_value
                    key = key.strip()
                    value = value.strip()
                    os.environ.setdefault(key, value)
                    print(f"[DEBUG] Loaded key: {key}")
    else:
        print(f"[DEBUG] .env file NOT found.")