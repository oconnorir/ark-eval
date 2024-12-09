# DATABASE CONNECTION
from db_connection import start_db_connection


def get_overdrawn_accounts():
	# CONNECT TO DB
	connection = start_db_connection()
	# CREATE A CURSOR OBJECT TO EXECUTE QUERIES
	cursor = connection.cursor()

	try:
		output_file_path = "overdrawn_accounts.txt"
		with open(output_file_path, "w") as file:
			file.write("Overdrawn Accounts Report\n")
			file.write("=" * 40 + "\n")

			# GET DISTINCT ACCOUNT GUID
			# IN CASE THERE ARE DUPLICATES
			distinct_account_guids = "SELECT DISTINCT account_guid FROM accounts;"
			cursor.execute(distinct_account_guids)
			account_ids = cursor.fetchall()

			# LOOP OVER ACCOUNTS IN TRANSACTIONS
			for account in account_ids:
				account_guid = account[0]

				# GET AND SUM ALL TRANSACTIONS BASED ON ACCOUNT GUID
				get_transaction_sums = f"""
					SELECT
						account_guid,
						SUM(transaction_amount) FROM transactions AS balance
					WHERE account_guid = '{account_guid}'
					GROUP BY account_guid;
				"""
				cursor.execute(get_transaction_sums)
				transaction_sums = cursor.fetchall()

				# PRINTS SUM OF TRANSACTIONS
				# [(account_guid, total_of_transactions)]
				# print(transaction_sums)

				# LOOP OVER SUMS
				for total in transaction_sums:
					account_guid = total[0]
					subtotal_from_transactions = total[1]

					# IF THERE IS A DEFICIT (SUM IS LESS THAN 0)
					if subtotal_from_transactions < 0:
						# QUERY CHECKING TABLE AGAINST ACCOUNT GUID
						# IN ORDER TO HAVE STARTING BALANCE BY ACCOUNT
						checking_starting_balance = f"SELECT * FROM checking WHERE account_guid = '{account_guid}';"
						cursor.execute(checking_starting_balance)
						values_from_checking = cursor.fetchall()

						# ENSURE LIST CONTAINS RESULTS
						if values_from_checking:
							starting_balance_from_checking = values_from_checking[0][1]
							deficit_from_transactions = total[1]
							difference_from_starting_balance = starting_balance_from_checking + deficit_from_transactions

							# IF THE DIFFERENCE IS LESS THAN 0
							if difference_from_starting_balance < 0:
								# QUERY MEMBERS TABLE BASED ON MEMBER
								# GUID FROM ACCOUNTS TABLE
								# THIS IS CROSS-REFERENCED
								# BY THE ACCOUNT GUID
								account_guid = values_from_checking[0][0]
								query_member_by_id = f"""
									SELECT
										CONCAT(first_name, ' ', last_name),
										memb.member_guid,
										accts.account_guid
									FROM members memb
									INNER JOIN accounts accts ON memb.member_guid = accts.member_guid
									WHERE accts.account_guid = '{account_guid}';
								"""
								cursor.execute(query_member_by_id)
								members = cursor.fetchall()
								if members:
									member_name = members[0][0]
									member_account_id = members[0][2]
									overdrawn_amount = abs(difference_from_starting_balance)

									# Write the details to the file
									file.write(f"Member: {member_name}\n")
									file.write(f"Account ID: {member_account_id}\n")
									file.write(f"Overdrawn Amount: {overdrawn_amount:.2f}\n")
									file.write("-" * 40 + "\n")
									print(f"""{members[0][0]} is overdrawn on their account (account id: {members[0][2]}).\nThe overdrawn amount is: {abs(difference_from_starting_balance)}\n""")

	except Exception as e:
		print(f"There was an error preventing complete function execution: {e}")
	finally:
		cursor.close()
		connection.close()


get_overdrawn_accounts()
