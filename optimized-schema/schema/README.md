# Druga verzija šeme baza podataka

## Druga verzija šeme baze podataka sastoji se iz 4 stare kolekcije i nove denormalizovane kolekcije koja je napravljena prema upitima, radi bržeg izvršavanja:

![Intial shema](intialschemascreenshot.jpg)

1. **Kolekcija customers** sadrži podatke o kupcima.

![Customers shema](customers.jpg)

2. **Kolekcija employees** sadrži podatke o zaposlenima.

![Employees shema](employees.jpg)

3. **Kolekcija products** sadrži podatke o proizvodima.

![Products shema](products.jpg)

4. **Kolekcija sales** sadrži podatke o prodajama proizvoda.

![Sales shema](sales.jpg)

5. **Kolekcija denormalized_sales sadrži podatke neophodne za izvršavanje osmišljenih upita bez lookup i unwind funckija**

![Denormalized shema](denormalized_sales.jpg)
