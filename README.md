✨ Created by Ashutosh Rai ✨
sqlserver2postgres-schema-validator ensures smooth and accurate data migration from SQL Server to PostgreSQL

# Data Reconciliator - v25.14.1

A scalable, user-friendly tool for validating and reconciling data between SQL Server and PostgreSQL databases. Designed for data migration, ETL, and audit scenarios, it provides robust row-by-row comparison, composite/primary key normalization, datatype compatibility, and detailed reporting—all with a modern Tkinter GUI.
<img width="1920" height="1080" alt="drcapp" src="https://github.com/user-attachments/assets/f9cc24f6-8ba6-4a92-83c9-fd86be957331" />


---

## Features

- **Cross-Database Validation:**
  - Compare data between SQL Server and PostgreSQL tables, including composite and primary key handling.
  - Supports both PK and non-PK tables with robust normalization logic.
- **Datatype Compatibility:**
  - Handles equivalent datatypes (e.g., `varchar` vs `character varying`, `int` vs `integer`, `nan` vs `None`) to avoid false mismatches.
- **Batch Processing:**
  - Efficiently processes large tables in configurable batches.
- **Flexible Output:**
  - Export mismatches to CSV or store results in a PostgreSQL table.
- **Comprehensive Logging:**
  - Table-level and summary logs, with color-coded log display in the UI.
- **Global Notifications:**
  - Clear, global notifications for all major actions (test connection, table update, validation completion, errors).
- **Modern UI:**
  - Tkinter-based interface with sidebar navigation, progress bar, and responsive layout.
- **Configurable:**
  - Save/load connection settings, batch size, and output preferences.
- **Error Handling:**
  - Graceful handling of connection, schema, and data errors with user feedback.

---

## Installation

1. **Clone the repository:**
   ```sh
   git clone https://github.com/codewithashurai/sqlserver2posresql-data-reconciliator.git
   cd sqlserver2posresql-data-reconciliator
   ```
2. **Install dependencies:**
   ```sh
   pip install -r requirements.txt
   ```
   - Requires Python 3.8 or higher.
   - Key dependencies: `pandas`, `sqlalchemy`, `psycopg2`, `pyodbc`, `tkinter` (standard with Python), `threading`.

---

## Usage

1. **Launch the Application:**
   ```sh
   python ui_main.py
   ```
2. **Configure Connections:**
   - Enter SQL Server and PostgreSQL connection details in the UI.
   - Test connections using the ⚡Test buttons.
3. **Select Databases and Tables:**
   - Choose source/target databases from dropdowns.
   - Select tables to validate (supports multi-select with checkboxes).
   - Click 'Update Table' to confirm selection.
4. **Set Batch Size and Output Format:**
   - Adjust batch size for large tables.
   - Choose output: CSV or Table Store.
5. **Start Validation:**
   - Click 'Start Validation 🚀' to begin.
   - Monitor progress and logs in real time.
   - Cancel anytime with the 'Cancel' button.
6. **Review Results:**
   - View summary and detailed logs in the UI.
   - Export results as needed.

---

## Output

- **CSV Reports:**
  - Mismatched rows exported to CSV in the configured output directory.
- **Database Table:**
  - Optionally, mismatches are stored in a PostgreSQL table (`public.DataReconciliator_schemas ,public.DataReconciliator_details`).
- **Logs:**
  - All actions, warnings, and errors are logged and viewable in the UI.

---

## Configuration

- **Persistent Settings:**
  - Connection details, batch size, and output preferences are saved for future sessions.
- **Output Directory:**
  - Set the output directory for reports/logs in the Settings page.

---

## Troubleshooting

- **Connection Issues:**
  - Use the ⚡Test buttons to verify credentials and network access.
  - Ensure required drivers (`ODBC Driver 17 for SQL Server`, `psycopg2` for PostgreSQL) are installed.
- **Data Mismatches:**
  - Review logs for normalization or datatype compatibility notes.
  - Adjust batch size for very large tables if memory issues occur.
- **UI Problems:**
  - If the UI does not display correctly, ensure you are using a supported Python version and OS.

---

## Contributing

Contributions are welcome! Please open issues or submit pull requests for bug fixes, enhancements, or documentation improvements.

---

## License

This project is licensed under the MIT License.

---

