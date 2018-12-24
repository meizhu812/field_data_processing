def progress_bar(progress: int, target: int):
    """

    :param progress:
    :param target:
    """
    percent_5 = int((progress / target) * 20)
    print('[' + percent_5 * '*' + (20 - percent_5) * ' ' + ']' + str(percent_5*5) + '%\r', end='', flush=True)
