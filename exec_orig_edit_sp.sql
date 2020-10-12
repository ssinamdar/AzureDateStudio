SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [CLI].[exec_orig_edit_sp1](
	@pi_process_id bigint
	,@pi_edit_type varchar(30)
	,@po_sql_code int OUT
	,@po_return_msg varchar(2000) OUT
)
AS
-- MODIFICATION HISTORY
-- Person			 Date			Comments
-- ---------------	 -----------    -----------------------------------------------------
-- Subhrajit Ghosh   09/08/2020		Created procedure. CLI Oracle to T-SQL conversion.

BEGIN
	--Staging table variables
	DECLARE @v_edit_id varchar(30),@v_edit_id123 varchar(30)--chilbranch123
		,@v_edit_set_id int
		,@v_substitution_value varchar(30)
		,@v_severity varchar(30)
		,@v_override_allowed_ind char(1)
		-------
		,@v_originator_symbol varchar(30)
		,@v_mortgage_type varchar(10)
		,@v_txn_type varchar(30)
		,@v_num_sql_code int
		,@v_chr_return_msg varchar(2000)

	SELECT @v_edit_id = edit_id
		,@v_edit_set_id = edit_set_id
		,@v_substitution_value = substitution_value
		,@v_severity = severity
		,@v_override_allowed_ind = override_allowed_ind
	FROM [stg].[oecr_edit_set_ctrl_rec]
	WHERE PROCESS_ID = @pi_process_id

	SELECT @v_originator_symbol = originator_symbol
		,@v_mortgage_type = mortgage_type
		,@v_txn_type = txn_type
	FROM [stg].[oecr_loan_control_rec]
	WHERE PROCESS_ID = @pi_process_id

    /* DEFINE CURSOR TO GET EDIT ROW FOR EDIT_ID PARAMETER */
	DECLARE origination_edits_cur CURSOR FAST_FORWARD READ_ONLY FOR
        SELECT edit_id, edit_msg, table_name, display_columns, edit_sql, valid_values_sql, edit_type
        FROM [CLI].[ORIGINATION_EDITS] ed
        WHERE ed.edit_id = @v_edit_id
			AND (LOWER(ed.edit_type) LIKE ('%' + @pi_edit_type + '%'))

	DECLARE @v_num_process_id bigint
		,@v_SYSDATE date = GETDATE()
		,@v_chr_SYSDATE varchar(30) = FORMAT(GETDATE(), 'MM/dd/yyyy HH:mm:ss')
		,@v_loans_with_errors_rec [CLI].[loans_with_errors_rec]
		--,@v_loc_edit_set_ctrl_rec      edit_set_ctrl_rec%TYPE = pio_orig_edit_control_rec.v_edit_set_ctrl_rec
		,@v_chr_db_pgm varchar(250) = 'exec_orig_edit_sp'
		,@v_chr_process_msg varchar(200) = NULL

		-- native dynamic sql
		,@v_select_expression varchar(2000) = ''
		,@v_insert varchar(2000) = ''
		,@v_select_set_edit varchar(4000) = '' 
		,@v_select_resolve_edit varchar(4000) = '' 
		,@v_sql varchar(max) = ''

		--@v_loans_with_errors_rec variables
		,@v_lwer_process_id bigint
		,@v_lwer_loan_id varchar(30)
		,@v_lwer_cli_status varchar(30)
		,@v_lwer_originator_symbol varchar(30)
		,@v_lwer_mortgage_type varchar(30)
		,@v_lwer_txn_type varchar(30)
		,@v_lwer_edit_set_id int
		,@v_lwer_edit_id varchar(30)
		,@v_lwer_edit_date date
		,@v_lwer_edit_msg varchar(2000)
		,@v_lwer_edit_type varchar(30)
		,@v_lwer_severity varchar(30)
		,@v_lwer_override_allowed_ind char(1)
		,@v_lwer_table_name varchar(30)
		,@v_lwer_display_columns varchar(500)
		,@v_lwer_display_data varchar(2000)
		,@v_lwer_modified_user_id varchar(20)
		,@v_lwer_modified_date date
		,@v_lwer_valid_values varchar(2000)
		,@v_lwer_status varchar(100)
		,@v_lwer_row_number smallint

	SET @v_chr_process_msg = @v_chr_db_pgm + ' being executed'

	BEGIN TRY
		SET @po_sql_code = 0
		SET @po_return_msg = NULL
		SET @v_num_process_id = @pi_process_id

		DECLARE @v_chr_in_value_list varchar(2000) = ''
		,@v_chr_out_value_list varchar(2000) = ''
		,@v_chr_edit_type varchar(30) = LOWER(LTRIM(RTRIM(@pi_edit_type)))
		,@v_num_edit_set_id int = @v_edit_set_id

		,@v_chr_originator_symbol varchar(30) = LOWER(LTRIM(RTRIM(@v_originator_symbol)))
		,@v_chr_mortgage_type varchar(10) = LOWER(LTRIM(RTRIM(@v_mortgage_type)))
		,@v_chr_txn_type varchar(30) = LOWER(LTRIM(RTRIM(@v_txn_type)))

		OPEN origination_edits_cur
			DECLARE @edit_id varchar(30), @edit_msg varchar(2000), @table_name varchar(100), @display_columns varchar(500)
				,@edit_sql varchar(2000), @valid_values_sql varchar(500), @edit_type varchar(30)
			FETCH NEXT FROM origination_edits_cur INTO @edit_id, @edit_msg, @table_name, @display_columns, @edit_sql, @valid_values_sql, @edit_type
			WHILE @@FETCH_STATUS = 0
			BEGIN
				DECLARE @v_chr_edit_sql varchar(2000) = LTRIM(RTRIM(@edit_sql))
				,@v_chr_valid_values_sql varchar(500) = LTRIM(RTRIM(@valid_values_sql))
				,@v_chr_substitution_value varchar(30) = LTRIM(RTRIM(isnull(@v_substitution_value, 'NULL')))
				,@v_chr_edit_msg varchar(2000) = REPLACE(@edit_msg, char(39), char(39) + char(39)) -- replace any single quote with a double single quote

				IF (isnull(@v_substitution_value, 0) > 0)
				BEGIN
					SET @v_chr_edit_sql = REPLACE(@v_chr_edit_sql, '?', @v_chr_substitution_value)
					SET @v_chr_valid_values_sql = REPLACE(@v_chr_valid_values_sql, '?', @v_chr_substitution_value)
				END

				SET @v_select_set_edit = 'SELECT process_id, loan_id, ''error'', originator_symbol, mortgage_type, txn_type, ' 
										 + @v_num_edit_set_id + ',' + '''' + @edit_id + '''' + @v_chr_SYSDATE 
										 + @v_chr_edit_msg + '''' + ',' + '''' + @edit_type + '''' + ',' + '''' + @v_severity + '''' + ',' + '''' 
										 + @v_override_allowed_ind + '''' + ',' + '''' + @table_name + '''' + ',' + '''' + @display_columns + '''' 
										 + ', NULL, modified_user_id, ' + @v_chr_SYSDATE
										 + @v_chr_in_value_list + '''' + ', status, row_number' + 				 						 				 
										 ' FROM ' + @table_name  + 
										 ' WHERE process_id = ' + @v_num_process_id + 
										 ' AND cli_status = ''success'' ' + 
										 ' AND txn_type = ' + '''' + @v_chr_txn_type + ''''  + 
										 ' AND originator_symbol = ' + '''' + @v_chr_originator_symbol + ''''  + 
										 ' AND mortgage_type = ' + '''' + @v_chr_mortgage_type + ''''  + 
										 ' AND (' + '' + @v_chr_edit_sql + '' + ')'

				SET @v_select_resolve_edit = 'SELECT process_id, loan_id, cli_status, originator_symbol, mortgage_type, txn_type, ' 
											+ @v_num_edit_set_id + ',' + '''' + @edit_id + '''' + @v_chr_SYSDATE
											+ @v_chr_edit_msg + '''' + ',' + '''' + @edit_type + '''' + ',' + '''' + @v_severity + '''' + ',' + '''' 
											+ @v_override_allowed_ind + '''' + ',' + '''' + @table_name + '''' + ',' + '''' 
											+ @display_columns + '''' + ', NULL, modified_user_id, ' + @v_chr_SYSDATE
											+ @v_chr_in_value_list + '''' + ', status, row_number' +             				
											' FROM ' + @table_name  + ' loan' + 
											' WHERE loan.process_id = ' + @v_num_process_id + 
											' AND loan.cli_status = ''success'' ' + 
											' AND loan.txn_type = ' + '''' + @v_chr_txn_type + ''''  + 
											' AND loan.originator_symbol = ' + '''' + @v_chr_originator_symbol + '''' + 
											' AND loan.mortgage_type = ' + '''' + @v_chr_mortgage_type + '''' + 
											' AND NOT (' + '' + @v_chr_edit_sql + '' + ')' + 
											-- see if edit previously set on the loans
											' AND EXISTS ' + 
											' (SELECT sub.loan_id ' + 
											' FROM loans_with_errors sub ' + 
											' WHERE sub.loan_id = loan.loan_id ' + 
											' AND isnull(sub.row_number, 0) = NVL(loan.row_number, 0) ' + 
											' AND sub.edit_id = ' + '' + '''' + @edit_id + '''' + ')'
				
				BEGIN TRY
					SET @v_sql = 'DECLARE v_cursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR ' + @v_select_set_edit
					EXEC (@v_sql)

					OPEN v_cursor
					FETCH NEXT FROM v_cursor INTO @v_lwer_process_id
													,@v_lwer_loan_id
													,@v_lwer_cli_status
													,@v_lwer_originator_symbol
													,@v_lwer_mortgage_type
													,@v_lwer_txn_type
													,@v_lwer_edit_set_id
													,@v_lwer_edit_id
													,@v_lwer_edit_date
													,@v_lwer_edit_msg
													,@v_lwer_edit_type
													,@v_lwer_severity
													,@v_lwer_override_allowed_ind
													,@v_lwer_table_name
													,@v_lwer_display_columns
													,@v_lwer_display_data
													,@v_lwer_modified_user_id
													,@v_lwer_modified_date
													,@v_lwer_valid_values
													,@v_lwer_status
													,@v_lwer_row_number

											INSERT INTO @v_loans_with_errors_rec
											SELECT @v_lwer_process_id
													,@v_lwer_loan_id
													,@v_lwer_cli_status
													,@v_lwer_originator_symbol
													,@v_lwer_mortgage_type
													,@v_lwer_txn_type
													,@v_lwer_edit_set_id
													,@v_lwer_edit_id
													,@v_lwer_edit_date
													,@v_lwer_edit_msg
													,@v_lwer_edit_type
													,@v_lwer_severity
													,@v_lwer_override_allowed_ind
													,@v_lwer_table_name
													,@v_lwer_display_columns
													,@v_lwer_display_data
													,@v_lwer_modified_user_id
													,@v_lwer_modified_date
													,@v_lwer_valid_values
													,@v_lwer_status
													,@v_lwer_row_number

					WHILE @@FETCH_STATUS = 0
					BEGIN
						IF (@display_columns IS NOT NULL)
						BEGIN
							EXEC [CLI].[format_edit_display_select_sp] @v_loans_with_errors_rec
																	,@v_select_expression OUT
																	,@po_sql_code OUT
																	,@po_return_msg OUT

							IF (@po_sql_code <> 0) 
								BREAK

							--The values for the record @v_loans_with_errors_rec is updated in the below proc.
							--This is achieved using staging table [stg].[loans_with_errors_rec]
							EXEC [CLI].[get_edit_display_data_sp] @v_loans_with_errors_rec
																,@v_select_expression
																,@po_sql_code OUT
																,@po_return_msg OUT
							
							DELETE FROM @v_loans_with_errors_rec

							INSERT INTO @v_loans_with_errors_rec
							SELECT * FROM [stg].[loans_with_errors_rec]
							WHERE PROCESS_ID = @pi_process_id

							IF (@po_sql_code <> 0)
								BREAK
						END

						IF (@v_chr_valid_values_sql IS NOT NULL)
						BEGIN
							SET @v_select_expression = @v_chr_valid_values_sql

							--The values for the record @v_loans_with_errors_rec is updated in the below proc.
							--This is achieved using staging table [stg].[loans_with_errors_rec]
							EXEC [CLI].[get_edit_valid_values_sp] @v_loans_with_errors_rec
																,@v_select_expression
																,@po_sql_code OUT
																,@po_return_msg OUT

							DELETE FROM @v_loans_with_errors_rec

							INSERT INTO @v_loans_with_errors_rec
							SELECT * FROM [stg].[loans_with_errors_rec]
							WHERE PROCESS_ID = @pi_process_id

							IF (@po_sql_code <> 0)
								BREAK
						END

						EXEC [CLI].[loans_with_errors_ins_sp] @v_loans_with_errors_rec
															,@po_sql_code OUT
															,@po_return_msg OUT
						IF (@po_sql_code <> 0)
							BREAK

						DECLARE @lv_edit_id varchar(30)
						SELECT @lv_edit_id = edit_id FROM @v_loans_with_errors_rec
						PRINT ('DEBUG:  success loans_with_errors_ins_sp.  edit_id is ' + @lv_edit_id)

						DELETE @v_loans_with_errors_rec
						FETCH NEXT FROM v_cursor INTO @v_lwer_process_id
														,@v_lwer_loan_id
														,@v_lwer_cli_status
														,@v_lwer_originator_symbol
														,@v_lwer_mortgage_type
														,@v_lwer_txn_type
														,@v_lwer_edit_set_id
														,@v_lwer_edit_id
														,@v_lwer_edit_date
														,@v_lwer_edit_msg
														,@v_lwer_edit_type
														,@v_lwer_severity
														,@v_lwer_override_allowed_ind
														,@v_lwer_table_name
														,@v_lwer_display_columns
														,@v_lwer_display_data
														,@v_lwer_modified_user_id
														,@v_lwer_modified_date
														,@v_lwer_valid_values
														,@v_lwer_status
														,@v_lwer_row_number
					END	
					CLOSE v_cursor
					DEALLOCATE v_cursor
					
					-------------------------------
					SET @v_sql = 'DECLARE v_cursor CURSOR LOCAL FAST_FORWARD READ_ONLY FOR ' + @v_select_resolve_edit
					EXEC (@v_sql)

					OPEN v_cursor
					FETCH NEXT FROM v_cursor INTO @v_lwer_process_id
													,@v_lwer_loan_id
													,@v_lwer_cli_status
													,@v_lwer_originator_symbol
													,@v_lwer_mortgage_type
													,@v_lwer_txn_type
													,@v_lwer_edit_set_id
													,@v_lwer_edit_id
													,@v_lwer_edit_date
													,@v_lwer_edit_msg
													,@v_lwer_edit_type
													,@v_lwer_severity
													,@v_lwer_override_allowed_ind
													,@v_lwer_table_name
													,@v_lwer_display_columns
													,@v_lwer_display_data
													,@v_lwer_modified_user_id
													,@v_lwer_modified_date
													,@v_lwer_valid_values
													,@v_lwer_status
													,@v_lwer_row_number

										INSERT INTO @v_loans_with_errors_rec
											SELECT @v_lwer_process_id
													,@v_lwer_loan_id
													,@v_lwer_cli_status
													,@v_lwer_originator_symbol
													,@v_lwer_mortgage_type
													,@v_lwer_txn_type
													,@v_lwer_edit_set_id
													,@v_lwer_edit_id
													,@v_lwer_edit_date
													,@v_lwer_edit_msg
													,@v_lwer_edit_type
													,@v_lwer_severity
													,@v_lwer_override_allowed_ind
													,@v_lwer_table_name
													,@v_lwer_display_columns
													,@v_lwer_display_data
													,@v_lwer_modified_user_id
													,@v_lwer_modified_date
													,@v_lwer_valid_values
													,@v_lwer_status
													,@v_lwer_row_number

					WHILE @@FETCH_STATUS = 0
					BEGIN
						EXEC [CLI].[loans_with_errors_resolved_sp] @v_loans_with_errors_rec
															,@po_sql_code OUT
															,@po_return_msg OUT

						IF (@po_sql_code <> 0)
							BREAK

						DELETE @v_loans_with_errors_rec
						FETCH NEXT FROM v_cursor INTO @v_lwer_process_id
														,@v_lwer_loan_id
														,@v_lwer_cli_status
														,@v_lwer_originator_symbol
														,@v_lwer_mortgage_type
														,@v_lwer_txn_type
														,@v_lwer_edit_set_id
														,@v_lwer_edit_id
														,@v_lwer_edit_date
														,@v_lwer_edit_msg
														,@v_lwer_edit_type
														,@v_lwer_severity
														,@v_lwer_override_allowed_ind
														,@v_lwer_table_name
														,@v_lwer_display_columns
														,@v_lwer_display_data
														,@v_lwer_modified_user_id
														,@v_lwer_modified_date
														,@v_lwer_valid_values
														,@v_lwer_status
														,@v_lwer_row_number
					END	
					CLOSE v_cursor
					DEALLOCATE v_cursor
				END TRY
				BEGIN CATCH
					SET @v_num_sql_code = ERROR_NUMBER()
					SET @v_chr_return_msg = ERROR_MESSAGE()

					PRINT(@v_chr_db_pgm + ' - ' + @v_chr_return_msg + '. EXCEPTION dynamic_sql_excep - edit_id is: ' + (@v_edit_id))
					SET @v_chr_process_msg = @v_chr_db_pgm + ' - ' + @v_chr_return_msg + '. EXCEPTION dynamic_sql_excep - edit_id is: ' + (@v_edit_id)
	
					EXEC [CLI].[orig_edit_ctrl_excep_sp] @pi_process_id
														,@v_chr_txn_type
														,@v_chr_db_pgm
														,@v_num_sql_code
														,@v_chr_process_msg
														,@v_num_sql_code OUT
														,@v_chr_return_msg OUT
					SET @po_sql_code = 0  -- return success when user exception
				END CATCH

				FETCH NEXT FROM origination_edits_cur INTO @edit_id, @edit_msg, @table_name, @display_columns, @edit_sql, @valid_values_sql, @edit_type
			END	
		CLOSE origination_edits_cur
		DEALLOCATE origination_edits_cur
	END TRY
	BEGIN CATCH
		IF (CURSOR_STATUS('local', 'v_cursor') <> -1)
		BEGIN
			CLOSE v_cursor
			DEALLOCATE v_cursor
		END

 		SET @po_sql_code = ERROR_NUMBER()
		SET @po_return_msg = ERROR_MESSAGE()

		PRINT(@v_chr_db_pgm + ' - ' + @po_return_msg + '. EXCEPTION OTHERS - edit_id is: ' + (@v_edit_id))
		SET @v_chr_process_msg = @po_return_msg + ' - ' + '. EXCEPTION OTHERS - edit_id is: ' + (@v_edit_id)	

		EXEC [CLI].[orig_edit_loan_excep_sp] @v_loans_with_errors_rec
											,@v_chr_db_pgm
											,@po_sql_code
											,@v_chr_process_msg
											,@v_num_sql_code OUT
											,@v_chr_return_msg OUT
	END CATCH
END
GO
