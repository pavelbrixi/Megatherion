from abc import abstractmethod, ABC
from json import load
from numbers import Real
from pathlib import Path
from typing import Dict, Iterable, Iterator, Tuple, Union, Any, List, Callable
from enum import Enum
from collections.abc import MutableSequence


class Type(Enum):
    Float = 0
    String = 1


def to_float(obj) -> float:
    ### PŘETYPUJE OBJEKT NA FLOAT, A POKUD JE OBJEKT None TAK OBJĚKT ZŮSTANE None
    ### DOSLOVA: if obj is not None:
    ###             return float(obj)
    ###          else:
    ###             return None
    return float(obj) if obj is not None else None


def to_str(obj) -> str:
    ### PŘETYPUJE OBJEKT NA STRING, A POKUD JE OBJEKT None TAK OBJĚKT ZŮSTANE None
    ### DOSLOVA: if obj is not None:
    ###             return str(obj)
    ###          else:
    ###             return None
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
        ### DATATYPE TOHOTO SLOUPCE BUDE TYP ZAPSANÝ V dtype 
        self.dtype = dtype
        ### CAST SE STARÁ O TO ABY "PROJEL" HODNOTY ZADANÉ UŽIVATELEM A ZJISTIL JESTLI NENÍ TŘEBA TEXT V DATATYPU FLOAT
        ### DOSLOVA: if self.dtype == Type.Float:
        ###             self._cast = to_float
        ###          else:
        ###             self._cast = to_str
        self._cast = to_float if self.dtype == Type.Float else to_str
        # cast function (it casts to floats for Float datatype or
        # to strings for String datattype)
        ### DATA GENERUJE ZE ZADANÝCH DAT SEZNAM, A POMOCÍ CAST "PROJÍŽDÍ" HODNOTY
        ### DOSLOVA: for value in data:
        ###             self._data.append(self._data(value))
        self._data = [self._cast(value) for value in data]

    def __len__(self) -> int:
        """
        Implementation of abstract base class `MutableSequence`.
        :return: number of rows
        """
        return len(self._data)

    def __getitem__(self, item: Union[int, slice]) -> Union[float, str, list[str], list[float]]:
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

    def permute(self, indices: List[int]) -> 'Column':
        """
        Return new column which items are defined by list of indices (to original column).
        (eg. `Column(["a", "b", "c"]).permute([0,0,2])`
        returns  `Column(["a", "a", "c"])
        :param indices: list of indexes (ints between 0 and len(self) - 1)
        :return: new column
        """
        assert len(indices) == len(self)
        ...

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
        return format(self._data[index], f"{width}s" if self.dtype == Type.String else f"-{width}.2g")

class DataFrame:
    """
    Dataframe with typed and named columns
    """
    def __init__(self, columns: Dict[str, Column]):
        """
        :param columns: columns of dataframe (key: name of dataframe),
                        lengths of all columns has to be the same
        """
        ### POKUD UŽIVATEL NAPÍŠE DATAFRAME BEZ SLOUPCŮ TAK PROGRAM VYHODÍ VYJÍMKU ŽE DATAFRAME BEZ SLOUPŮ NENÍ PODPOROVÁN
        assert len(columns) > 0, "Dataframe without columns is not supported"
        ### SIZE SE STARÁ O TO ABY SLOPCE BYLY STEJNĚ DLOUHÉ, POKUD NE TAK FUNKCE common VYHODÍ HLÁŠKU "Not all values are the same"
        self._size = common(len(column) for column in columns.values())
        # deep copy od dict `columns`
        ### COLUMNS ZAPISUJE VŠE DO "DATABÁZE" (slovníku)
        self._columns = {name: column.copy() for name, column in columns.items()}

    def __getitem__(self, index: int) -> Tuple[Union[str,float]]:
        """
        Indexed getter returns row of dataframe as tuple
        :param index: index of row
        :return: tuple of items in row
        """
        ...

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

    def append_column(self, col_name:str, column: Column) -> None:
        """
        Appends new column to dataframe (its name has to be unique).
        :param col_name:  name of new column
        :param column: data of new column
        """
        if col_name in self._columns:
            raise ValueError("Duplicate column name")
        self._columns[col_name] = column.copy()

    def append_row(self, row: Iterable) -> None:
        """
        Appends new row to dataframe.
        :param row: tuple of values for all columns
        """

        if len(row) != len(self._columns):
            raise ValueError("Počet zadaných hodnot neodpovídá počtu sloupců")
        

        for column_name, value in zip(self._columns.keys(), row):
            data = self._columns.get(column_name, [])
            if type(value) == int:
                value = float(value)
            if value == None:
                self._columns[column_name].append(None)  
            elif type(data[0]) != type(value):
                print("Nový řádek obsahuje špatný datový typ, hodnota bude nahrazena None")
                self._columns[column_name].append(None)
            else:
                self._columns[column_name].append(value)   

        self._size = self._size + 1
            
    #def filter(self, col_name:str, predicate: Callable[[Union[float, str]], bool]) -> 'DataFrame':
    def filter(self, items:list) -> 'DataFrame':
        """
        Returns new dataframe with rows which values in column `col_name` returns
        True in function `predicate`.

        :param col_name: name of tested column
        :param predicate: testing function
        :return: new dataframe
        """
        filtered_dict = {}

        while items:
            for klic in items:
                if klic in self._columns:
                    filtered_dict[klic] = self._columns.get(klic)
                    items.remove(klic)
                else:
                    print(f"Hodnota", klic, "v dataframe neexistuje")
                    items.remove(klic)
                break

        print(DataFrame(filtered_dict))

        ### Alternativa:
        ### return DataFrame(filtered_dict)
        ### při
        ### print(df.filter(["a", "c"]))

    def sort(self, col_name:str, ascending=True) -> 'DataFrame':
        """
        Sort dataframe by column with `col_name` ascending or descending.
        :param col_name: name of key column
        :param ascending: direction of sorting
        :return: new dataframe
        """
        ### KONTROLA ZDA UŽIVATEL ZADAL EXISTUJÍCÍ SLOUPEC
        if col_name not in self._columns.keys():
            raise TypeError("Zadaný sloupec neexistuje")

        ### DO PROMĚNÉ data SE ULOŽÍ DATA ZE ZADANÉHO SLOUPCE
        data = self._columns.get(col_name, [])

        ### VYPÍŠE UTŘÍDĚNÁ DATA NA OBRAZOVKU 
        if ascending == True:
            print(col_name)
            for value in sorted(data):
                print(value)
        else: 
            print(col_name)
            for value in sorted(data, reverse=True):
                print(value)

    def describe(self) -> str:
        """
        similar to pandas but only with min, max and avg statistics for floats and count"
        :return: string with formatted decription
        """
        described_data = []

        for column_name in self._columns.keys():
            data = list(self._columns.get(column_name, []))
            if isinstance(data[1], float):
                described_data.extend(data)
        
        print(f"max: ", max(described_data))
        print(f"min: ", min(described_data))
        print(f"avg: ", sum(described_data) / len(described_data))


    def inner_join(self, other: 'DataFrame', self_key_column: str,  other_key_column: str) -> 'DataFrame':
        """
            Inner join between self and other dataframe with join predicate
            `self.key_column == other.key_column`.

            Possible collision of column identifiers is resolved by prefixing `_other` to
            columns from `other` data table.
        """
        ...

    def setvalue(self, col_name: str, row_index: int, value: Any) -> None:
        """
        Set new value in dataframe.
        :param col_name:  name of culumns
        :param row_index: index of row
        :param value:  new value (value is cast to type of column)
        :return:
        """
        ### DO PROMĚNÉ col SE ULOŽÍ KLÍČ SLOVNÍKU _columns
        col = self._columns[col_name]
        ### ZADANÁ HODNOTA SE PROJEDE FUNKCÍ _cast A ULOŽÍ SE DO ZVOLENÉ POZICE V KOLEKCI _data
        col[row_index] = col._cast(value)

    @staticmethod
    def read_csv(path: Union[str, Path]) -> 'DataFrame':
        """
        Read dataframe by CSV reader
        """
        return CSVReader(path).read()

    @staticmethod
    def read_json(path: Union[str, Path]) -> 'DataFrame':
        """
        Read dataframe by JSON reader
        """
        return JSONReader(path).read()
    
    
    def size(self):

        ### VRÁTÍ CELKOVÝ POČET POLOŽEK

        print(len(self) * len(self._columns))

    def head(self, size:int = 5) -> 'DataFrame':

        if size > self._size:
            raise ValueError("Zadaná velikost je příliš velká")

        head = {}

        for column_name in self._columns.keys():
            data = list(self._columns.get(column_name, []))
            head[column_name] = Column(data[:size], Type.String)

        return DataFrame(head)

    def tail(self, size:int = 5) -> 'DataFrame':

        if size > self._size:
            raise ValueError("Zadaná velikost je příliš velká")
        
        size -= 1

        tail = {}

        for column_name in self._columns.keys():
            data = list(self._columns.get(column_name, []))
            tail[column_name] = Column(data[size:], Type.String)

        return DataFrame(tail)
    
    def at(self, location:list):

        if location[0] > self._size:
            raise ValueError("Zadaný index v dataframe neexistuje")
        elif location[1] not in self._columns:
            raise ValueError("Zadaný sloupec v dataframe neexistuje")
        else:
            data = list(self._columns.get(location[1], []))
            value = data[location[0] - 1]

        return value
    
    def pop(self, target):
        self._columns.pop(target, [])

    def add(self, value:int) -> 'DataFrame':

        for i in self._columns.keys():       
            if (self._columns.get(i))._cast == to_str:
                pass
            else:
                data = self._columns.get(i, [])
                data = [hodnota + value for hodnota in data]
                self._columns[i] = Column(data, Type.Float)

    def sub(self, value:int) -> 'DataFrame':

        for i in self._columns.keys():       
            if (self._columns.get(i))._cast == to_str:
                pass
            else:
                data = self._columns.get(i, [])
                data = [hodnota - value for hodnota in data]
                self._columns[i] = Column(data, Type.Float)

    def suma(self) ->  'DataFrame':

        for column in self._columns.keys():
            mezivypocet = 0
            if (self._columns.get(column))._cast == to_float:
                data = list(self._columns.get(column, []))
                for polozka in data:
                    mezivypocet += polozka
                data.append(mezivypocet)
                self._columns[column] = Column(data, Type.Float)
            else:
                data = list(self._columns.get(column, []))
                data.append(None)
                self._columns[column] = Column(data, Type.String)

        self._size += 1

class Reader(ABC):
    def __init__(self, path: Union[Path, str]):
        self.path = Path(path)

    @abstractmethod
    def read(self) -> DataFrame:
        raise NotImplemented("Abstract method")


class JSONReader(Reader):
    """
    Factory class for creation of dataframe by CSV file. CSV file must contain
    header line with names of columns.
    The type of columns should be inferred from types of their values (columns which
    contains only value has to be floats columns otherwise string columns),
    """
    def read(self) -> DataFrame:
        with open(self.path, "rt") as f:
            json = load(f)
        columns = {}
        for cname in json.keys(): # cyklus přes sloupce (= atributy JSON objektu)
            dtype = Type.Float if all(value is None or isinstance(value, Real)
                                      for value in json[cname]) else Type.String
            columns[cname] = Column(json[cname], dtype)
        return DataFrame(columns)


class CSVReader(Reader):
    """
    Factory class for creation of dataframe by JSON file. JSON file must contain
    one object with attributes which array values represents columns.
    The type of columns are inferred from types of their values (columns which
    contains only value is floats columns otherwise string columns),
    """
    def read(self) -> 'DataFrame':
        ...


if __name__ == "__main__":
    ### ZÁKLADNÍ ZOBRAZENÍ DATAFRAME 
    ### [jméno nového dataframe] = Dataframe(dict(
    ###     [jméno nového řádku] = Column([ [hodnoty (oddělené čárku)] ], Type.[typ hodnot (Float nebo String)]
    ###     ))
    df = DataFrame(dict(
        a=Column([1, 3.1415], Type.Float),
        b=Column(["a", 2], Type.String),
        c=Column(range(2), Type.Float)
        ))
    
    #zvirata = DataFrame(dict(
    #    animal=Column(['alligator', 'bee', 'falcon', 'lion', 'monkey', 'parrot', 'shark', 'whale', 'zebra'], Type.String),
    #    neco=Column(['yx', 'as', 'qw', 'er', 'df', 'cv', 'bn', 'gh', 'zu'], Type.String)
    #    ))
    
    ### ZMĚNA HODNOTY V DANÉM SLOPCI 
    ### [jméno dataframe].setvalue("[jméno sloupce], [index řádku (začíná se od 0)], [zadaná hodnota]")
    #df.setvalue("a", 1, 42)

    ### PŘIDÁNÍ NOVÉHO SLOUPCE
    ### [jméno dataframe].append_column([jméno nového řádku] = Column([ [hodnoty (oddělené čárku)] ], Type.[typ hodnot (Float nebo String)])
    #df.append_column("d", Column([60, 55], Type.Float))

    ### PŘIDÁNÍ NOVÉHO ŘÁDKU
    ### [jméno dataframe].append_row([seznam nových hodnot])
    #df.append_row([1, "None", 1, 5])

    ### FILTER SLOUPCŮ
    ### [jméno dataframe].filter([seznam názvů sloupců])
    #df.filter(["a", "c"])

    ### VÝPIS DATAFRAME
    ### print([jméno dataframe])
    #print(df)

    ### SEŘAZENÍ JEDNOTLIVÉHO SLOUPCE
    ### [jméno dataframe].sort([jméno sloupce], [True - vzestupně, False - sestupně])
    #df.sort("a", False)

    ### DESCRIBE VYPÍŠE MAXIMÁLNÍ, MINIMÁLNÍ A PRŮMĚRNOU HODNOTU ZE VŠECH FLOAT HODNOT V DATAFRAME
    #df.describe()

    ### VRÁTÍ CELKOVÝ POČET POLOŽEK
    #df.size()

    ### VÝTAH DAT Z JSON SOUBORU
    ### [jméno dataframe] = Dataframe.read_json("[jméno json souboru]")
    #df = DataFrame.read_json("data.json")
    #print(df)

    ### VÝPIS PRVNÍCH PĚTI ZÁZNAMŮ (SPOLEČNĚ SE JMÉNEM SLOUPCE)
    ### print([název dataframe].head([požadovaný počet položek / KDYŽ NEVYPLNĚNO -> 5]))
    #print(zvirata.head())

    ### VÝPIS POSLEDNÍCH PĚTI ZÁZNAMŮ (SPOLEČNĚ SE JMÉNEM SLOUPCE)
    ### print([název dataframe].tail([požadovaný počet položek / KDYŽ NEVYPLNĚNO -> 5]))
    #print(zvirata.tail())

    ### VÝTAH KONKRÉTNÍ HODNOTY Z DATAFRAME
    ### print([jméno dataframe].at([[index řádku], [název sloupce]))
    #print(df.at([2, "a"]))

    ### VYMAZÁNÍ KONKRÉTNÍHO SLOUPCE
    ### [jméno dataframe].pop("[jméno sloupce]")
    #df.pop("b")

    ### PŘIČTENÍ ZADANÉ HODNOTY KE VŠEM ČÍSELNÝM POLOŽKÁM
    ### [jméno dataframe].add([sčítanec])
    #df.add(1)

    ### ODEČTENÍ ZADANÉ HODNOTY KE VŠEM ČÍSELNÝM POLOŽKÁM
    ### [jméno dataframe].add([menšitel])
    #df.sub(1)

    #df.suma()

    print(df)

### VÝPIS VŠECH ŘÁDKŮ V DATAFRAMU df
# for line in df:
#     print(line)

###
