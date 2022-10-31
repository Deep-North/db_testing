import unittest
import logging
import psycopg2
from psycopg2 import Error

import parameters

params = parameters.Parameters()

logging.basicConfig(level=logging.INFO, filename="log.txt",filemode="w",
                    format="%(asctime)s %(levelname)s %(message)s")

class DB_Positive_Tests(unittest.TestCase):
    """DB positive testing.
    """

    @classmethod
    def setUpClass(cls):
        cls.connection = psycopg2.connect(user=params.user[0],
                                          password=params.password[0],
                                          host=params.host[0],
                                          port=params.port[0],
                                          database=params.database)
        cls.connection.set_client_encoding("UTF8")
        cls.cursor = cls.connection.cursor()
        logging.info("Connection 1 to PostgreSQL established")

    @classmethod
    def tearDownClass(cls):
        if cls.connection:
            cls.cursor.close()
            cls.connection.close()
            logging.info("Connection 1 to PostgreSQL closed")

    def setUp(self) -> None:
        self.sql_execution('''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                VALUES
                                (1, 'Andrey', '1984-01-07'),
                                (2, 'Ivan', '1990-11-17'),
                                (3, 'John Doe', '2015-04-12'),
                                (4, 'Jane Doe', '2000-01-25'),
                                (5, 'Peter', '1934-07-09')
                                ;''')

    def tearDown(self) -> None:
        self.sql_execution('''DELETE FROM public."People" WHERE "Index" IN (1,2,3,4,5);''')

    ##################################    TESTS    ###################################

    def test_select_query(self):
        try:
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth"
                                 FROM public."People"
                                 WHERE "Index" > 4 and "DateOfBirth" = '1934-07-09';''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)
        self.assertEqual(output, "['5', 'Peter', '1934-07-09']")

    def test_insert_query(self):
        try:
            self.sql_execution('''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                    VALUES (6, 'test_data', '2000-02-02');''')
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."People" WHERE "Index" = 6;''')

            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)

            self.assertEqual(output, "['6', 'test_data', '2000-02-02']")

            self.sql_execution('''DELETE FROM public."People" WHERE "Index" = 6;''')
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."People";''')

        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)

    def test_update_query(self):
        try:
            self.sql_execution('''UPDATE public."People" SET "Index"='101', "Name"='New Name' WHERE "Index" = 1;''')
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."People" WHERE "Index" = 101;''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)

            self.assertEqual(output, "['101', 'New Name', '1984-01-07']")

            self.sql_execution('''UPDATE public."People" SET "Index"='1', "Name"='Andrey' WHERE "Index" = 101;''')
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)

    def test_delete_query(self):
        try:
            self.sql_execution('''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                    VALUES (6, 'test_data', '2000-02-02');''')
            self.sql_execution('''DELETE FROM public."People" WHERE "Index" = 6;''')

            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."People" WHERE "Index" = 6;''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)
        self.assertEqual(output, "[]")

    #############################    UTILITY METHODS    ##############################

    def query_output(self, query_result):
        output = []
        for i in query_result:
            for j in i:
                output.append(str(j))
        return str(output)

    def sql_execution(self, query_body):
        try:
            query = query_body
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.commit()
            raise Exception


class DB_Negative_Tests(unittest.TestCase):
    """DB negative testing.
    """

    @classmethod
    def setUpClass(cls):
        cls.connection = psycopg2.connect(user=params.user[0],
                                          password=params.password[0],
                                          host=params.host[0],
                                          port=params.port[0],
                                          database=params.database)
        cls.connection.set_client_encoding("UTF8")
        cls.cursor = cls.connection.cursor()
        logging.info("Connection 2 to PostgreSQL established")

    @classmethod
    def tearDownClass(cls):
        if cls.connection:
            cls.cursor.close()
            cls.connection.close()
            logging.info("Connection 2 to PostgreSQL closed")

    def setUp(self) -> None:
        self.sql_execution('''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                VALUES
                                (1, 'Andrey', '1984-01-07'),
                                (2, 'Ivan', '1990-11-17'),
                                (3, 'John Doe', '2015-04-12'),
                                (4, 'Jane Doe', '2000-01-25'),
                                (5, 'Peter', '1934-07-09')
                                ;''')

    def tearDown(self) -> None:
        self.sql_execution('DELETE FROM public."People" WHERE "Index" IN (1,2,3,4,5);')

    ##################################    TESTS    ###################################

    def test_select_nonexisting_row(self):
        self.assertRaises(Exception, self.sql_execution, '''SELECT "Index1" FROM public."People";''')

    def test_insert_with_excess_number_of_fields(self):
        self.assertRaises(Exception, self.sql_execution, '''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                                            VALUES
                                                            (10, 'Андрей', '1984-01-07');''')

    def test_insert_with_NULL_to_NOT_NULL_field(self):
        self.assertRaises(Exception, self.sql_execution, '''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                                            VALUES
                                                            (NULL, 'Андрей', '1984-01-07');''')

    def test_insert_with_incorrect_data_type(self):
        self.assertRaises(Exception, self.sql_execution, '''INSERT INTO public."People"("Index", "Name", "DateOfBirth")
                                                            VALUES
                                                            ('1984-01-07', 'Андрей', '1984-01-07');''')

    def test_update_with_incorrect_data_type(self):
        self.assertRaises(Exception, self.sql_execution, '''UPDATE public."People"
                                                            SET "Index"='a1'
                                                            WHERE "Index" = 1;''')

    def test_update_with_NULL_to_NOT_NULL_field(self):
        self.assertRaises(Exception, self.sql_execution, '''UPDATE public."People"
                                                            SET "Index"=NULL
                                                            WHERE "Index" = 1;''')

    def test_update_with_excess_number_of_fields(self):
        self.assertRaises(Exception, self.sql_execution, '''UPDATE public."People"
                                                            SET "Index"='101', "Name"='Andrey',
                                                            "DateOfBirth"='1984-01-07', "FakeColumn"='123'
                                                            WHERE "Index" = 1;''')

    def test_delete_from_nonexisting_fields(self):
        self.assertRaises(Exception, self.sql_execution, '''DELETE FROM public."1DateOfBirth"
                                                            WHERE "Index" IN (1,2,3,4,5,6);''')

    #############################    UTILITY METHODS    ##############################

    def query_output(self, query_result):
        output = []
        for i in query_result:
            for j in i:
                output.append(str(j))
        return str(output)

    def sql_execution(self, query_body):
        try:
            query = query_body
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.commit()
            raise Exception

class DB_Additional_Tests(unittest.TestCase):
    """DB additional positive testing.
    """

    @classmethod
    def setUpClass(cls):
        cls.connection = psycopg2.connect(user=params.user[0],
                                          password=params.password[0],
                                          host=params.host[0],
                                          port=params.port[0],
                                          database=params.database)
        cls.connection.set_client_encoding("UTF8")
        cls.cursor = cls.connection.cursor()
        logging.info("Connection 3 to PostgreSQL established")

    @classmethod
    def tearDownClass(cls):
        if cls.connection:
            cls.cursor.close()
            cls.connection.close()
            logging.info("Connection 3 to PostgreSQL closed")

    def setUp(self) -> None:
        self.sql_execution('''INSERT INTO public."Test_Table"("Index", "Name", "DateOfBirth")
                                VALUES
                                (1, 'Andrey', '1984-01-07'),
                                (2, 'Ivan', '1990-11-17'),
                                (3, 'John Doe', '2015-04-12'),
                                (4, 'Jane Doe', '2000-01-25'),
                                (5, 'Peter', '1934-07-09')
                                ;''')

    def tearDown(self) -> None:
        self.sql_execution('''DELETE FROM public."Test_Table" WHERE "Index" IN (1,2,3,4,5);''')

    ##################################    TESTS    ###################################

    def test_select_with_multiple_conditions_query(self):
        try:
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth"
                                    FROM public."Test_Table"
                                    WHERE "Index" > 2 and "DateOfBirth" > '1970-01-01';''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)
        self.assertEqual(output, "['3', 'John Doe', '2015-04-12', '4', 'Jane Doe', '2000-01-25']")

    def test_update_with_LIKE_condition_query(self):
        try:
            self.sql_execution('''UPDATE public."Test_Table"
                                    SET "Name"='New Name'
                                    WHERE "Test_Table"."Name" LIKE '%e%';''')
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."Test_Table";''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)

            self.assertEqual(output, '''['2', 'Ivan', '1990-11-17', '1', 'New Name', '1984-01-07', '3', 'New Name', '2015-04-12', '4', 'New Name', '2000-01-25', '5', 'New Name', '1934-07-09']''')

            self.sql_execution('''UPDATE public."Test_Table" SET "Index"='1', "Name"='Andrey' WHERE "Index" = 101;''')
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)

    def test_delete_with_LIKE_condition_query(self):
        try:
            self.sql_execution('''DELETE FROM public."Test_Table"
                                    WHERE "Test_Table"."Name" LIKE '%J%';''')
            self.sql_execution('''SELECT "Index", "Name", "DateOfBirth" FROM public."Test_Table";''')
            query_result = self.cursor.fetchall()
            output = self.query_output(query_result)
        except (Exception, Error) as error:
            print("Error during work with PostgreSQL:\n", error)
        self.assertEqual(output, "['1', 'Andrey', '1984-01-07', '2', 'Ivan', '1990-11-17', '5', 'Peter', '1934-07-09']")

    #############################    UTILITY METHODS    ##############################

    def query_output(self, query_result):
        output = []
        for i in query_result:
            for j in i:
                output.append(str(j))
        return str(output)

    def sql_execution(self, query_body):
        try:
            query = query_body
            self.cursor.execute(query)
            self.connection.commit()
        except:
            self.connection.commit()
            raise Exception


if __name__ == '__main__':
    unittest.main()
