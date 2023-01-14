from pathlib import Path
import sys
import logging


logging.basicConfig(
    format="%(levelname)s: %(asctime)s - %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)

path_root = Path(__file__).parents[2]
sys.path.append(str(path_root))

from src.ApacheBeamBaseInteraction import VirginMediaTestOne, VirginMediaTestTwo

ABBase_client = VirginMediaTestOne()
ABBase_client.process()

ABBase_client = VirginMediaTestTwo()
ABBase_client.process()