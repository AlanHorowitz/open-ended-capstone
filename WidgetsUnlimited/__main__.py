import sys

from RetailDW.demo import demo1, demo2, demo3


def main():
    """Run a RetailDW demo from the command line.

    Usage: python -m RetailDW [demo name]
    """
    demos_available = {
        "demo1": (demo1, "Initial and incremental load of source system"),
        "demo2": (demo2, "Load of source system.  ETL to target system"),
        "demo3": (demo3, "Load 3 tables to source system.  ETL to target system"),
    }

    if len(sys.argv) > 1:
        arg1 = sys.argv[1]
        if arg1 in demos_available:
            demo = demos_available[arg1][0]
            demo()
            return
        else:
            print(f"{arg1} not an available demo.")

    print("Usage: python -m RetailDW [demo name]")
    print("Available demo(s) are:")
    print(*demos_available.keys())


if __name__ == "__main__":
    main()
