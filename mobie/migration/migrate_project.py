import argparse
from .migrate_v1.migrate import migrate_to_mobie as migrate_project_v1
from .migrate_v2 import migrate_project as migrate_project_v2


def main():
    parser = argparse.ArgumentParser(description="Migrate project to newer spec version.")
    parser.add_argument('root', type=str)
    msg = """Migration script version: choose one of
        1) migrate from platybrowser spec to MoBIE spec 0.1
        2) migrate spec 0.1 to 0.2"""
    parser.add_argument('--version', '-v', type=int, default=2, help=msg)
    parser.add_argument('--pattern', '-p', type=str, default='*')
    parser.add_argument('--anon', '-a', type=int, default=1)
    parser.add_argument('--update_view_spec', '-u', default=0, type=int)

    args = parser.parse_args()
    version = args.version
    if version == 1:
        migrate_project_v1(args.root, args.pattern, bool(args.anon))
    elif version == 2:
        migrate_project_v2(args.root, update_view_spec=bool(args.update_view_spec))
    else:
        raise ValueError(f"Invalid version {version}")


if __name__ == '__main__':
    main()
