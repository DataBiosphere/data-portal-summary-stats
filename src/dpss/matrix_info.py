from pathlib import Path
from typing import (
    Optional,
    FrozenSet,
)

from attr import dataclass
from more_itertools import one


@dataclass
class MatrixInfo:
    source: str
    project_uuid: str
    zip_path: Optional[Path]
    extract_path: Path
    lib_con_approaches: FrozenSet[str] = frozenset()

    @property
    def figures_folder(self) -> str:
        try:
            lca = one(self.lib_con_approaches)
        except ValueError:
            raise RuntimeError(f'Should not upload figures for matrix {self.project_uuid}'
                               'because it has not been separated by library construction approach.')
        return f'{self.project_uuid}/{lca}/'
