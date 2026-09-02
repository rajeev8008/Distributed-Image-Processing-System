from pathlib import Path
from typing import Iterable

from PIL import Image

from app.services.splitter import TileInfo


def convert_tiles_to_grayscale(tiles: Iterable[TileInfo], output_dir: Path) -> list[TileInfo]:
    output_dir.mkdir(parents=True, exist_ok=True)
    processed = []
    for tile in tiles:
        output_path = output_dir / f"{tile.index}.png"
        with Image.open(tile.path) as image:
            image.convert("L").save(output_path, "PNG")
        processed.append(TileInfo(tile.index, tile.x, tile.y, tile.width, tile.height, output_path))
    return processed


def merge_tiles(tiles: Iterable[TileInfo], size: tuple[int, int], output_path: Path) -> None:
    result = Image.new("L", size)
    for tile in tiles:
        with Image.open(tile.path) as image:
            if image.size != (tile.width, tile.height):
                raise ValueError(f"Tile {tile.index} dimensions do not match its metadata")
            result.paste(image, (tile.x, tile.y))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result.save(output_path, "PNG")

