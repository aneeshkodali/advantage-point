import subprocess

def get_current_branch():
    """
    Returns branch currently checked out
    """

    # get the current git branch
    branch = subprocess.check_output(
        ['git', 'rev-parse', '--abbrev-ref', 'HEAD'],
        stderr=subprocess.STDOUT
    ).strip().decode('utf-8')

    return branch