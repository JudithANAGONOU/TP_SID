#Importation des modules 
import pandas as pd
import mysql.connector
from mysql.connector import Error

#Chargement des données
sales_data = pd.read_excel('Sales_Transactions_Dataset_Weekly.xlsx', sheet_name='Sales_Transactions_Dataset_Week')
print(sales_data.columns)


# Vérification des données en vue d'un traitement si nécessaire
print(sales_data.info())
sales_data = sales_data.drop(['MIN','MAX'], axis = 1) # Suppression des colonnes Min et Max
print('----------------------------------------------------------------------------------')
print(sales_data.columns)


# Extraction du nom des colonnes contenues dans la base de données
colonnes = ['id_produit','week1','week2','week3','week4','week5','week6','week7','week8','week9','week10','week11','week12','week13','week14','week15','week16','week17','week18','week19','week20','week21','week22','week23','week24','week25','week26','week27','week28','week29','week30','week31','week32','week33','week34','week35','week36','week37','week38','week39','week40','week41','week42','week43','week44','week45','week46','week47','week48','week49','week50','week51','week52']
sales_data.columns = colonnes 
print('----------------------------------------------------------------------------------')
print(sales_data.columns)


# Chargement du dataframe dans MYSQL
try:
    connexion = mysql.connector.connect(host='localhost',
                                       database='Sales_transaction',
                                       user='root',
                                       password='')
    if connexion.is_connected():
        print('----------------------------------------------------------------------------------')
        print('Connexion à MySQL réussie')
except Error as e:
    print('----------------------------------------------------------------------------------')
    print(f"Erreur lors de la connexion à MySQL: {e}")

try:
    cursor = connexion.cursor() 
    cursor.execute('''
                CREATE TABLE IF NOT EXISTS transaction(
                   id_produit VARCHAR(6) PRIMARY KEY,
                   week1 INT NOT NULL,
                   week2 INT NOT NULL,
                   week3 INT NOT NULL,
                   week4 INT NOT NULL,
                   week5 INT NOT NULL,
                   week6 INT NOT NULL,
                   week7 INT NOT NULL,
                   week8 INT NOT NULL,
                   week9 INT NOT NULL,
                   week10 INT NOT NULL,
                   week11 INT NOT NULL,
                   week12 INT NOT NULL,
                   week13 INT NOT NULL,
                   week14 INT NOT NULL,
                   week15 INT NOT NULL,
                   week16 INT NOT NULL,
                   week17 INT NOT NULL,
                   week18 INT NOT NULL,
                   week19 INT NOT NULL,    
                   week20 INT NOT NULL,
                   week21 INT NOT NULL,
                   week22 INT NOT NULL,
                   week23 INT NOT NULL,
                   week24 INT NOT NULL,
                   week25 INT NOT NULL,
                   week26 INT NOT NULL,
                   week27 INT NOT NULL,
                   week28 INT NOT NULL,
                   week29 INT NOT NULL,
                   week30 INT NOT NULL,
                   week31 INT NOT NULL,
                   week32 INT NOT NULL,
                   week33 INT NOT NULL,
                   week34 INT NOT NULL,
                   week35 INT NOT NULL,
                   week36 INT NOT NULL,
                   week37 INT NOT NULL,
                   week38 INT NOT NULL,
                   week39 INT NOT NULL,    
                   week40 INT NOT NULL,
                   week41 INT NOT NULL,
                   week42 INT NOT NULL,
                   week43 INT NOT NULL,
                   week44 INT NOT NULL,
                   week45 INT NOT NULL,
                   week46 INT NOT NULL,
                   week47 INT NOT NULL,
                   week48 INT NOT NULL,
                   week49 INT NOT NULL,
                   week50 INT NOT NULL,
                   week51 INT NOT NULL,
                   week52 INT NOT NULL
                );
                ''')

    for i,row in sales_data.iterrows():
        sql = """INSERT INTO transaction (id_produit,week1,week2,week3,week4,week5,week6,week7,week8,week9,week10,week11,week12,week13,week14,week15,week16,week17,week18,week19,week20,week21,week22,week23,week24,week25,week26,week27,week28,week29,week30,week31,week32,week33,week34,week35,week36,week37,week38,week39,week40,week41,week42,week43,week44,week45,week46,week47,week48,week49,week50,week51,week52) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        #print(sql)
        cursor.execute(sql, tuple(row))

    connexion.commit()
    connexion.close()
    print('----------------------------------------------------------------------------------')
    print("DataFrame chargé dans MySQL avec succès!")
except Exception as e:
    print('----------------------------------------------------------------------------------')
    print(f"Erreur lors du chargement du DataFrame dans MySQL: {e}")



