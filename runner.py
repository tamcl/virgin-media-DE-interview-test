import argparse
from pathlib import Path
import logging
import sys

logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.ApacheBeamBaseInteraction import VirginMediaTestOne, VirginMediaTestTwo

def main(args):
    param = {}
    if args.input_path:
        param.update({"input_url_src":args.input_path})

    if args.output_path:
        param.update({"local_output_dest":args.output_path})

    if args.task_number == 1:
        VirginMediaTestOne(**param).process()
    elif args.task_number == 2:
        VirginMediaTestTwo(**param).process()
    else:
        raise Exception('Task number error')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_path', type=str)
    parser.add_argument('--output_path', type=str)
    parser.add_argument('--task_number', type=int, required=True)
    args = parser.parse_args()
    main(args)