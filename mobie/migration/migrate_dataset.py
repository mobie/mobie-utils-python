import argparse
from .migrate_v1.migrate_dataset import migrate_dataset_to_mobie as migrate_dataset_v1
from .migrate_v2 import migrate_dataset as migrate_dataset_v2
from .migrate_v3 import migrate_dataset as migrate_dataset_v3


def main():
    parser = argparse.ArgumentParser(description="Migrate dataset to newer spec version.")
    parser.add_argument("folder", type=str)
    msg = """Migration script version: choose one of
        1) migrate from platybrowser spec to MoBIE spec 0.1
        2) migrate spec 0.1 to 0.2
        3) migrate spec 0.2 to 0.3"""
    parser.add_argument("--version", "-v", type=int, default=3, help=msg)
    parser.add_argument("--anon", type=int, default=1)

    args = parser.parse_args()
    version = args.version
    if version == 1:
        migrate_dataset_v1(args.folder, bool(args.anon))
    elif version == 2:
        migrate_dataset_v2(args.folder)
    elif version == 3:
        migrate_dataset_v3(args.folder)
    else:
        raise ValueError(f"Invalid version {version}")


if __name__ == "__main__":
    main()
