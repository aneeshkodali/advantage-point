import importlib

def main():
    
    # have list of scripts to run
    source_script_list = [
        'scripts.web.ingest__web__main',
    ]

    # loop through scripts
    for source_script_name in source_script_list:

        # import and execute
        source_script = importlib.import_module(source_script_name)
        source_script.main()


if __name__ == '__main__':
    main()