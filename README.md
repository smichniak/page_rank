# Page_rank

Trzecie duże zadanie zaliczeniowe z przedmiotu [Programowanie współbieżne](https://usosweb.mimuw.edu.pl/kontroler.php?_action=katalog2/przedmioty/pokazPrzedmiot&kod=1000-213bPW) w roku akademickim 2020/21 na wydziale MIM UW.

## Zrównoleglony Page Rank
### Wprowadzenie
PageRank jest algorytmem, który historycznie jest jedną z podstaw wyszukiwarki Google. Jego zadaniem jest przypisanie każdej stronie liczby, której wartość zależy od tego ile stron do niej linkuje oraz jaka jest wartość PageRank dla tych stron. Strony do których linkuje wiele wartościowych stron również są uważane za wartościowe.

Zadanie polega na zrównolegleniu i zoptymalizowaniu załączonej implementacji algorytmu PageRank oraz przeanalizowaniu jego wydajności.

### Opis problemu
W szablonie zadania znajduje się implementacja Strony (klasa Page) reprezentującej pojedynczą stronę w pewnym systemie do zarządzania wiedzą. Każda strona ma swoją treść (pole “content”) oraz zbiór linków do treści powiązanych. Ponieważ system jest adresowany treścią (ang. “content-addressable”) to każdy link jest w rzeczywistości hashem SHA256 wygenerowanym z treści strony do której się odnosi.

Wszystkie strony tworzą sieć (klasa Network) i zadaniem jest policzenie wartości PageRank dla każdej strony w sieci. Zgodnie z opisem na [wikipedii](https://pl.wikipedia.org/wiki/PageRank) wartość PageRank wyznacza się w następujący sposób

```PR_x = (1-d)/N + d*(PR_y/L_y + PR_z/L_z + ...)```

Gdzie:

```PR``` to wartośc PageRank danej strony 

```d``` to współczynnik tłumienia (zwykle 0.85)

```N``` to liczba stron w sieci

```L``` to liczba linków wychodzących z danej strony

W przypadku, gdy do strony nie ma żadnych linków wchodzących powyższa formuła jest niezdefiniowana. Istnieją liczne metody obsługi tych tzw. wiszących węzłów (ang. “dangling node”). Na potrzeby tego zadania przyjmijmy metodę wykorzystaną w popularnej bibliotece networkx (opisanej [tutaj](https://www.geeksforgeeks.org/page-rank-algorithm-implementation/)): dla każdego węzła suma ilorazów wartości PR i liczby linków wychodzących jest powiększona o sumę wartości PR dla stron bez linków wychodzących podzielonej przez liczbę węzłów w sieci.

Zadaniem studenta jest wyliczenie wartości PageRank metodą iteracyjną. W pierwszym kroku każda strona ma taki sam pagerank równy 1/n, gdzie n to liczba stron w sieci. W każdym następnym kroku wartość PageRank jest wyliczana w sposób zgodny z powyższym algorytmem, tak długo aż suma różnic wartości między dwoma kolejnymi krokami będzie bardzo mała (mniejsza niż parametr tolerance) lub liczba iteracji przekroczy wyznaczony limit (wtedy powinien zostać zwrócony błąd).

### Opis zadania
Bazą do zadania jest szablon (PageRank.tar dostępny na tej stronie) zawierający implementację wielu klas oraz liczne testy, w szczególności polecenie ./runTests.sh które uruchamia wszystkie testy. Pliki multiThreadedPageRankComputer.hpp oraz sha256IdGenerator.hpp wymagają uzupełnienia, natomiast singleThreadedPageRankComputer.hpp zawiera gotową, jednowątkową, implementację algorytmu wyliczającego wartości Page Rank. Implementacja jednowątkowa może zostać użyta jako baza dla rozwiązania równoległego. Dopuszczalne są również modyfikacje w pliku singleThreadedPageRankComputer.hpp np. w celu poprawienia wydajności rozwiązania jednowątkowego.

Pierwszym elementem zadania jest implementacja metody computeForNetwork dla klasy ```MultiThreadedPageRankComputer```, która w konstrukturze przyjmuje wartość numThreads oznaczającej ile dodatkowych wątków można wykorzystać podczas obliczeń. Celem zadania jest uzyskanie praktycznego przyspieszenia. Rozwiązanie zrównoleglone powinno działać istotnie szybciej - np. działać krócej w porównaniu do rozwiązania sekwencyjnego o 30%-50% w przypadku dwóch wątków, 50-75% krócej w przypadku czterech wątków. Osiągnięte rezultaty mogą się różnić w zależności od użytego sprzętu (znaczenie ma nie tylko procesor i jego liczba rdzeni, ale też pamięć RAM), dlatego należy testować program również na maszynie students. Oczekiwane jest rozwiązanie w którym obliczenia z jednym wątkiem dodatkowym mają wydajność porównywalną (albo nawet niższą z uwagi na dodatkowe narzuty) z wydajnością rozwiązania sekwencyjnego, więc wątek główny nie powinien wykonywać ciężkich obliczeń.

Rozwiązanie równoległe powinno używać wyłącznie mechanizmów z biblioteki standardowej C++14, ewentualnie systemowych funkcji języka C. Przykładami użytecznych klas są ```std::thread```, ```std::atomic```, ```std::lock_guard```. Operacjami dominującymi powinny być instrukcje związane z klasą ```PageId``` oraz kontenerami które ją przechowują. W celu ujednolicenia rozwiązań studentów klasa ta nie zawiera implementacji operatora <. Proszę więc nie przechowywać obiektów ```PageId``` w zbiorach i mapach uporządkowanych (```std::set```, ```std::map```), lecz w kontenerach używających hashowania (```std::unordered_set```, ```std::unordered_map```) przy użyciu zaimplementowanego już ```PageIdHash```.

Zgodnie z opisem problemu, link do strony jest w formie hasha SHA256 z jej zawartości. Zadaniem studenta jest również wyliczenie wartości tego hasha dla każdej strony, a konkretniej implementacji metody generateId w klasie ```Sha256IdGenerator```. Większość dystrybucji Linkusa (w tym maszyna students) udostępnia program sha256sum, więc oczekiwana implementacja metody generateId powinna uruchamiać nowy proces wykonujący program sha256sum. Każdy wątek powinien uruchomić co najwyżej jeden proces sha256sum chodzący jednocześnie.
