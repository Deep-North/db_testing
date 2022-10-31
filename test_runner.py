import unittest
import test_db

dbTestSuite = unittest.TestSuite()
dbTestSuite.addTest(unittest.makeSuite(test_db.DB_Positive_Tests))
dbTestSuite.addTest(unittest.makeSuite(test_db.DB_Negative_Tests))
dbTestSuite.addTest(unittest.makeSuite(test_db.DB_Additional_Tests))

print("count of tests: " + str(dbTestSuite.countTestCases()) + "\n")

runner = unittest.TextTestRunner(verbosity=2)
runner.run(dbTestSuite)
