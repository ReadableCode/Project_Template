def get_git_link(
    repo_owner, repo_name, branch_name, script_name, directory="src", subdirectory=None
):
    """
    Get the link to the git repository
    :return: link to the git repository
    """
    if subdirectory is None:
        return f"https://github.com/{repo_owner}/{repo_name}/blob/{branch_name}/{directory}/{script_name}"
    else:
        return f"https://github.com/{repo_owner}/{repo_name}/blob/{branch_name}/{directory}/{subdirectory}/{script_name}"
