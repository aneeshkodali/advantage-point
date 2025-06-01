from utils.functions.version_control.get_current_branch import get_current_branch

def format_env_value(
    value: str
):

    # get current branch
    current_branch = get_current_branch()

    # return value if 'production' branch
    if current_branch in ('main', 'master'):
        return value
    # otherwise return 'dev' value
    else:
        return f"{value}_dev"