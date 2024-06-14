from typing import Dict, Iterable, Iterator, Tuple, Union, Any, List
from enum import Enum
from collections.abc import MutableSequence

class Type(Enum):
    Float = 0
    String = 1


def to_float(obj) -> float:
    """
    casts object to float with support of None objects (None is cast to None)
    """
    return float(obj) if obj is not None else None


def to_str(obj) -> str:
    """
    casts object to float with support of None objects (None is cast to None)
    """
    return str(obj) if obj is not None else None


def common(iterable): # from ChatGPT
    """
    returns True if all items of iterable are the same.
    :param iterable:
    :return:
    """
    try:
        # Nejprve zkusíme získat první prvek iterátoru
        iterator = iter(iterable)
        first_value = next(iterator)
    except StopIteration:
        # Vyvolá výjimku, pokud je iterátor prázdný
        raise ValueError("Iterable is empty")

    # Kontrola, zda jsou všechny další prvky stejné jako první prvek
    for value in iterator:
        if value != first_value:
            raise ValueError("Not all values are the same")

    # Vrací hodnotu, pokud všechny prvky jsou stejné
    return first_value

class Column(MutableSequence):# implement MutableSequence (some method are mixed from abc)
    """
    Representation of column of dataframe. Column has datatype: float columns contains
    only floats and None values, string columns contains strings and None values.
    """
    def __init__(self, data: Iterable, dtype: Type):
        self.dtype = dtype
        self._cast = to_float if self.dtype == Type.Float else to_str
        # cast function (it casts to floats for Float datatype or
        # to strings for String datattype)
        self._data = [self._cast(value) for value in data]

    def __len__(self) -> int:
        """
        Implementation of abstract base class `MutableSequence`.
        :return: number of rows
        """
        return len(self._data)

    def __getitem__(self, item: Union[int, slice]) -> Union[float,
                                    str, list[str], list[float]]:
        """
        Indexed getter (get value from index or sliced sublist for slice).
        Implementation of abstract base class `MutableSequence`.
        :param item: index or slice
        :return: item or list of items
        """
        return self._data[item]

    def __setitem__(self, key: Union[int, slice], value: Any) -> None:
        """
        Indexed setter (set value to index, or list to sliced column)
        Implementation of abstract base class `MutableSequence`.
        :param key: index or slice
        :param value: simple value or list of values

        """
        self._data[key] = self._cast(value)

    def append(self, item: Any) -> None:
        """
        Item is appended to column (value is cast to float or string if is not number).
        Implementation of abstract base class `MutableSequence`.
        :param item: appended value
        """
        self._data.append(self._cast(item))

    def insert(self, index: int, value: Any) -> None:
        """
        Item is inserted to colum at index `index` (value is cast to float or string if is not number).
        Implementation of abstract base class `MutableSequence`.
        :param index:  index of new item
        :param value:  inserted value
        :return:
        """
        self._data.insert(index, self._cast(value))

    def __delitem__(self, index: Union[int, slice]) -> None:
        """
        Remove item from index `index` or sublist defined by `slice`.
        :param index: index or slice
        """
        del self._data[index]

    def copy(self) -> 'Column':
        """
        Return shallow copy of column.
        :return: new column with the same items
        """
        # FIXME: value is cast to the same type (minor optimisation problem)
        return Column(self._data, self.dtype)

    def get_formatted_item(self, index: int, *, width: int):
        """
        Auxiliary method for formating column items to string with `width`
        characters. Numbers (floats) are right aligned and strings left aligned.
        Nones are formatted as aligned "n/a".
        :param index: index of item
        :param width:  width
        :return:
        """
        assert width > 0
        if self._data[index] is None:
            if self.dtype == Type.Float:
                return "n/a".rjust(width)
            else:
                return "n/a".ljust(width)
        return format(self._data[index],
                      f"{width}s" if self.dtype == Type.String else f"-{width}.2g")

class Knihovna:

    def __init__(self, columns: Dict[str, Column]):

        assert len(columns) > 0, "Knihovna nemá žádná data"
        self._size = common(len(column) for column in columns.values())
    
        self._columns = {name: column.copy() for name, column in columns.items()}

    def __iter__(self) -> Iterator[Tuple[Union[str, float]]]:
        """
        :return: iterator over rows of dataframe
        """
        for i in range(len(self)):
            yield tuple(c[i] for c in self._columns.values())

    def __len__(self) -> int:
        """
        :return: count of rows
        """
        return self._size

    @property
    def columns(self) -> Iterable[str]:
        """
        :return: names of columns (as iterable object)
        """
        return self._columns.keys()

    def __repr__(self) -> str:
        """
        :return: string representation of dataframe (table with aligned columns)
        """
        sizes = []
        for column in self._columns.keys():
            data = list(self._columns.get(column))
            for polozka in data:
                sizes.append(len(str(polozka)))

        self.field_width = max(sizes)

        lines = []
        lines.append(" ".join(f"{name:{self.field_width}s}" for name in self.columns))
        for i in range(len(self)):
            lines.append(" ".join(self._columns[cname].get_formatted_item(i, width=self.field_width)
                                     for cname in self.columns))
        return "\n".join(lines)
        

    def pridat_zaznam(self, novy_zaznam:list):
        poradi = 0
        for column in self._columns.keys():
            data = list(self._columns.get(column))
            data.append(novy_zaznam[poradi])
            poradi += 1
            self._columns[column] = Column(data, Type.String)

        self._size += 1

    def odstranit_zaznam(self, target:int):
        for column in self._columns.keys():
            data = list(self._columns.get(column))
            data.remove(data[target - 1])
            self._columns[column] = Column(data, Type.String)

        self._size -= 1

    def window(self) -> str:

        sizes = []
        for column in self._columns.keys():
            data = list(self._columns.get(column))
            for polozka in data:
                sizes.append(len(str(polozka)))

        self.field_width = max(sizes)

        lines = []
        lines.append("┌" + "".join("─" * self.field_width + "┬") * (len(self.columns) - 1) + ("─" * self.field_width + "┐"))
        lines.append("│" +"│".join(f"{name:{self.field_width}s}" for name in self.columns) + "│")
        lines.append("├" +"".join("─" * self.field_width + "┼") * (len(self.columns) - 1) + ("─" * self.field_width + "┤"))
        pocet_polozek = self._size
        for i in range(len(self)):
            lines.append("│" + "│".join(self._columns[cname].get_formatted_item(i, width=self.field_width) for cname in self.columns) + "│")
            if pocet_polozek > 1:
                lines.append("├" + "".join("─" * self.field_width + "┼") * (len(self.columns) - 1) + ("─" * self.field_width + "┤"))
                pocet_polozek -= 1
            else:
                lines.append("└" + "".join("─" * self.field_width + "┴") * (len(self.columns) - 1) + ("─" * self.field_width + "┘"))
        return "\n".join(lines)


if __name__ == "__main__":

    knihovna = Knihovna(dict(
        author=Column([], Type.String),
        name=Column([], Type.String),
        year=Column([], Type.String)
        ))
        
    knihovna.pridat_zaznam(Column(["Božena Němcová", "Babička", "1855"], Type.String))
    knihovna.pridat_zaznam(Column(["Karel Čapek", "Válka s Mloky", "1936"], Type.String))
    knihovna.pridat_zaznam(Column(["Jaroslav Hašek", "Osudy dobrého vojáka Švejka", "1921"], Type.String))
    knihovna.pridat_zaznam(Column(["Bohumil Hrabal", "Postřižiny", "1976"], Type.String))

    #knihovna.odstranit_zaznam(1)

    #print(knihovna)
  
    print(knihovna.window())
