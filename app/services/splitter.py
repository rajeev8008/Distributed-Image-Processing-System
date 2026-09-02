from dataclasses import dataclass
from pathlib import Path

from PIL import Image


@dataclass(frozen=True)
class TileInfo:
    index: int
    x: int
    y: int
    width: int
    height: int
    path: Path


def split_image(image: Image.Image, output_dir: Path, tile_size: int = 512) -> list[TileInfo]:
    if tile_size <= 0:
        raise ValueError("Tile size must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)
    tiles: list[TileInfo] = []
    index = 0
    for y in range(0, image.height, tile_size):
        for x in range(0, image.width, tile_size):
            width = min(tile_size, image.width - x)
            height = min(tile_size, image.height - y)
            path = output_dir / f"{index}.png"
            image.crop((x, y, x + width, y + height)).save(path, "PNG")
            tiles.append(TileInfo(index, x, y, width, height, path))
            index += 1
    return tiles

