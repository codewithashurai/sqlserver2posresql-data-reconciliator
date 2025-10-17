"""
Tkinter UI for Scalable SQL Server ↔ PostgreSQL Data Validation Tool
"""
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
from tkinter.scrolledtext import ScrolledText
import threading
import queue
from config import AppConfig
from validation_engine import ValidationEngine
import os
import json
import sqlalchemy

class DataReconciliatorApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('RCW Data Reconciliator')
        self.geometry('1200x800')
        self.state('zoomed')
        self.configure(bg='#293891')
        self.style = ttk.Style(self)
        self._set_theme()
        self.ui_queue = queue.Queue()
        # Define log method before AppConfig
        # This ensures log_callback is valid
        self.global_log = []
        self.log_text_widget = None
        # Use the log method defined below
        self.config = AppConfig(log_callback=self.log)
        self.validation_thread = None
        self.stop_event = threading.Event()
        self.sql_db_list = []
        self.pg_db_list = []
        # --- Add missing Tkinter variables ---
        self.sql_host = tk.StringVar(value=self.config.sqlserver.get('server', ''))
        self.sql_db = tk.StringVar(value=self.config.sqlserver.get('db', ''))
        self.sql_user = tk.StringVar(value=self.config.sqlserver.get('user', ''))
        self.sql_pwd = tk.StringVar(value=self.config.sqlserver.get('pwd', ''))
        self.sql_driver = tk.StringVar(value=self.config.sqlserver.get('driver', 'ODBC Driver 17 for SQL Server'))
        self.sql_win_auth = tk.BooleanVar(value=self.config.sqlserver.get('win_auth', False))
        self.pg_host = tk.StringVar(value=self.config.postgres.get('server', ''))
        # --- Sanitize host field on startup ---
        if '@' in self.pg_host.get():
            corrected_host = self.pg_host.get().split('@')[-1]
            self.pg_host.set(corrected_host)
            self.config.postgres['server'] = corrected_host
            self.config.save()
        self.pg_db = tk.StringVar(value=self.config.postgres.get('db', ''))
        self.pg_user = tk.StringVar(value=self.config.postgres.get('user', ''))
        self.pg_pwd = tk.StringVar(value=self.config.postgres.get('pwd', ''))
        self.pg_port = tk.StringVar(value=self.config.postgres.get('port', '5432'))
        # --- End missing variables ---
        self._build_ui()
        self.after(100, self.process_ui_queue)

    def _set_theme(self):
        self.style.theme_use('clam')
        self.style.configure('TFrame', background='#293891')
        self.style.configure('TLabel', background='#293891', foreground='#ffffff', font=('Segoe UI', 10))
        # Standard button style: white bg, yellow text; hover: yellow bg, black text
        self.style.configure('TButton', background='#ffffff', foreground='#d4b461', font=('Segoe UI', 11, 'bold'))
        self.style.map('TButton', background=[('active', '#d4b461'), ('!active', '#ffffff')], foreground=[('active', '#000000'), ('!active', '#d4b461')])
        self.style.configure('TEntry', fieldbackground='#f5f5f5', foreground='#232323')
        self.style.configure('TCombobox', fieldbackground='#f5f5f5', background='#f5f5f5', foreground='#232323')
        self.style.map('TCombobox', fieldbackground=[('readonly', '#f5f5f5')], background=[('readonly', '#f5f5f5')])
        self.style.configure('Treeview', background='#232323', fieldbackground='#232323', foreground='#ffffff', font=('Segoe UI', 9), bordercolor='#c99700', borderwidth=1)
        self.style.configure('Treeview.Heading', background='', foreground='#c99700', font=('Segoe UI', 10, 'bold'), bordercolor='#c99700', borderwidth=1)
        self.style.map('Treeview.Heading', background=[('active', '')], bordercolor=[('active', '#c99700'), ('!active', '#c99700')])
        self.style.configure('TNotebook', background='#293891')
        self.style.configure('TNotebook.Tab', background='#c99700', foreground='#000000')
        self.style.map('TNotebook.Tab', background=[('selected', '#ffd700')], foreground=[('selected', '#000000')])
        self.style.configure('GrayGold.TButton', background='#ffffff', borderwidth=0.5, relief='solid', foreground='#d4b461', font=('Segoe UI', 10, 'bold'))
        self.style.map('GrayGold.TButton', background=[('active', '#d4b461'), ('!active', '#ffffff')], foreground=[('active', '#000000'), ('!active', '#d4b461')], bordercolor=[('active', '#d4b461'), ('!active', '#d4b461')])
        self.style.configure('RoundedGold.TButton', background='#ffffff', borderwidth=1, relief='groove', foreground='#d4b461', font=('Segoe UI', 10, 'bold'), padding=6)
        self.style.map('RoundedGold.TButton', background=[('active', '#d4b461'), ('!active', '#ffffff')], foreground=[('active', '#000000'), ('!active', '#d4b461')], bordercolor=[('active', '#d4b461'), ('!active', '#d4b461')])
        # Custom style for yellow-bordered label frames
        self.style.configure('YellowGroup.TLabelframe', background='#293891', bordercolor='#d4b461', borderwidth=2)
        self.style.configure('YellowGroup.TLabelframe.Label', foreground='#d4b461', background='#293891', font=('Segoe UI', 11, 'bold'))
        self.style.configure('YellowSection.TLabelframe', background='#293891', bordercolor='#d4b461', borderwidth=2)
        self.style.configure('YellowSection.TLabelframe.Label', foreground='#d4b461', background='#293891', font=('Segoe UI', 10, 'bold'))

    def _load_app_colors(self):
        settings_path = os.path.join(os.path.dirname(__file__), 'appsettings.config')
        presets = {
            'dark': {
                'bg': '#293891',
                'fg': '#ffffff',
                'button_bg': '#ffffff',
                'button_fg': '#d4b461',
                'accent': '#d4b461',
                'highlight': '#ffd700',
                'sidebar_bg': '#222a5c',
            },
            'light': {
                'bg': '#f5f5f5',
                'fg': '#232323',
                'button_bg': '#293891',
                'button_fg': '#ffd700',
                'accent': '#293891',
                'highlight': '#d4b461',
                'sidebar_bg': '#e0e0e0',
            }
        }
        if os.path.exists(settings_path):
            try:
                with open(settings_path, 'r') as f:
                    color_settings = json.load(f)
            except Exception:
                color_settings = presets['dark'].copy()
                color_settings['mode'] = 'dark'
        else:
            color_settings = presets['dark'].copy()
            color_settings['mode'] = 'dark'
        mode = color_settings.get('mode', 'dark')
        self.app_colors = presets[mode].copy()
        self.app_colors.update(color_settings)

    def _build_ui(self):
        self._load_app_colors()
        main_container = tk.Frame(self, bg=self.app_colors['bg'])
        main_container.pack(fill='both', expand=True)
        # Sidebar (vertical menu) always at far left
        self.sidebar_expanded = tk.BooleanVar(value=True)
        self.sidebar_frame = tk.Frame(main_container, bg=self.app_colors['sidebar_bg'], width=64)
        self.sidebar_frame.pack(side='left', fill='y')
        self._update_sidebar()
        # Right side: everything else
        right_container = tk.Frame(main_container, bg=self.app_colors['bg'])
        right_container.pack(side='left', fill='both', expand=True)
        # Top bar: starts after sidebar width
        top_bar = tk.Frame(right_container, bg=self.app_colors['bg'], height=40)
        top_bar.pack(fill='x', side='top')
        # Burger button in horizontal menu bar (left side)
        def toggle_sidebar():
            self.sidebar_expanded.set(not self.sidebar_expanded.get())
            self._update_sidebar()
        burger_btn = tk.Button(top_bar, text='☰', font=('Segoe UI', 16), bg=self.app_colors['bg'], fg=self.app_colors['accent'], bd=0, command=toggle_sidebar, relief='flat', activebackground=self.app_colors['bg'], activeforeground=self.app_colors['highlight'])
        burger_btn.pack(side='left', padx=(8, 0), pady=4)
        # Right side: About and Help as link-style buttons
        help_link = tk.Label(top_bar, text='📄', font=('Segoe UI', 11, 'underline'), fg=self.app_colors['accent'], bg=self.app_colors['bg'], cursor='hand2')
        help_link.pack(side='right', padx=(0, 8), pady=4)
        help_link.bind('<Button-1>', lambda e: self._show_help())
        # --- Refresh button (right side) ---
        def refresh_current_page():
            current_page = getattr(self, 'selected_menu', 'Home')
            if current_page == 'Report':
                # Refresh logic for Report page
                db_val = self.report_db_var.get().strip()
                if not db_val:
                    self.log('No database selected for report refresh.', success=False)
                    return
                self.config.postgres['db'] = db_val
                self.config.save()
                self._refresh_summary_grid()
            else:
                # General refresh for other pages (if needed)
                self.log('[INFO] Refreshing current page...', success=True)
                # You can add specific refresh logic for other pages here
        # refresh_btn = tk.Button(top_bar, text='Refresh', font=('Segoe UI', 10, 'bold'), bg=self.app_colors['accent'], fg='#000000', command=refresh_current_page, relief='groove', bd=1, padx=8, pady=2)
        # refresh_btn.pack(side='right', padx=(0, 8), pady=4)
        # Main content area below top bar
        self.content_frame = tk.Frame(right_container, bg=self.app_colors['bg'])
        self.content_frame.pack(fill='both', expand=True)
        # --- Global log system ---
        self.global_log = []
        self.log_text_widget = None
        # --- Global notification label (always present, hidden by default) ---
        self.global_notification = tk.Label(self, text='', bg=self.app_colors['bg'], fg='#ffffff', font=('Segoe UI', 9), anchor='w')
        self.global_notification.place_forget()
        self._show_page('Home')
        self._bind_config_fields()

    def _bind_config_fields(self):
        # Bind all config fields to update config.py on change
        def update_sqlserver(*_):
            self.config.sqlserver['server'] = self.sql_host.get()
            # self.config.sqlserver['db'] = self.sql_db.get()  # Remove DB update here
            self.config.sqlserver['user'] = self.sql_user.get()
            self.config.sqlserver['pwd'] = self.sql_pwd.get()
            self.config.sqlserver['driver'] = self.sql_driver.get()
            self.config.sqlserver['win_auth'] = self.sql_win_auth.get()
            self.config.save()
        def update_postgres(*_):
            self.config.postgres['server'] = self.pg_host.get()
            # self.config.postgres['db'] = self.pg_db.get()  # Remove DB update here
            self.config.postgres['user'] = self.pg_user.get()
            self.config.postgres['pwd'] = self.pg_pwd.get()
            self.config.postgres['port'] = self.pg_port.get()
            self.config.save()
        def update_batch_size(*_):
            try:
                self.config.batch_size = int(self.batch_size.get())
            except Exception:
                self.config.batch_size = 1000
            self.config.save()
        def update_output_type(*_):
            self.config.output_type = self.output_type.get()
            self.config.save()
        self.sql_host.trace_add('write', update_sqlserver)
        self.sql_db.trace_add('write', update_sqlserver)
        self.sql_user.trace_add('write', update_sqlserver)
        self.sql_pwd.trace_add('write', update_sqlserver)
        self.sql_driver.trace_add('write', update_sqlserver)
        self.sql_win_auth.trace_add('write', update_sqlserver)
        self.pg_host.trace_add('write', update_postgres)
        self.pg_db.trace_add('write', update_postgres)
        self.pg_user.trace_add('write', update_postgres)
        self.pg_pwd.trace_add('write', update_postgres)
        self.pg_port.trace_add('write', update_postgres)
        self.batch_size.trace_add('write', update_batch_size)
        self.output_type.trace_add('write', update_output_type)

    def _update_sidebar(self):
        for widget in self.sidebar_frame.winfo_children():
            widget.destroy()
        logo = tk.Label(self.sidebar_frame, text='🛢', font=('Segoe UI', 22, 'bold'), bg=self.app_colors['sidebar_bg'], fg=self.app_colors['accent'])
        logo.pack(pady=(16, 8))
        if self.sidebar_expanded.get():
            name = tk.Label(self.sidebar_frame, text='Data Reconciliator', font=('Segoe UI', 12, 'bold'), bg=self.app_colors['sidebar_bg'], fg=self.app_colors['fg'])
            name.pack(pady=(0, 2), anchor='w', padx=16)
            subtitle = tk.Label(self.sidebar_frame, text='Scalable SQL Server ↔ PostgreSQL Data Validation', font=('Segoe UI', 9, 'italic'), bg=self.app_colors['sidebar_bg'], fg='#c0c0c0', wraplength=180, justify='left')
            subtitle.pack(pady=(0, 2), anchor='w', padx=16)
            version = tk.Label(self.sidebar_frame, text='25.14.1', font=('Segoe UI', 9), bg=self.app_colors['sidebar_bg'], fg=self.app_colors['accent'])
            version.pack(pady=(0, 12), anchor='w', padx=16)
        self.selected_menu = getattr(self, 'selected_menu', 'Home')
        nav_items = [
            ('🏠', 'Home', lambda: self._select_menu('Home'), None),
            ('📊', 'Report', lambda: self._select_menu('Report'), None),
            ('⚙️', 'Settings', lambda: self._select_menu('Settings'), None),
            #('ⓘ', 'Help', lambda: self._show_help(), None),  # Changed icon to ⓘ
        ]
        for icon, txt, cmd, img in nav_items:
            def make_cmd(page=txt):
                return lambda: self._select_menu(page)
            if self.sidebar_expanded.get():
                if img:
                    btn = tk.Label(
                        self.sidebar_frame,
                        image=img,
                        text=f'  {txt}',
                        compound='left',
                        font=('Segoe UI', 12, 'bold'),
                        bg=self.app_colors['sidebar_bg'],
                        fg=self.app_colors['fg'],
                        padx=8,
                        pady=8,
                        anchor='w',
                    )
                else:
                    btn = tk.Label(
                        self.sidebar_frame,
                        text=f'{icon} {txt}',
                        font=('Segoe UI', 12, 'bold'),
                        bg=self.app_colors['sidebar_bg'],
                        fg=self.app_colors['fg'],
                        padx=8,
                        pady=8,
                        anchor='w',
                    )
                btn.pack(fill='x', padx=8, pady=2)
                btn.bind('<Button-1>', lambda e, page=txt: self._select_menu(page))
                def on_enter(event, b=btn):
                    if self.selected_menu != txt:
                        b.config(bg=self.app_colors['bg'], fg=self.app_colors['highlight'])
                def on_leave(event, b=btn):
                    if self.selected_menu != txt:
                        b.config(bg=self.app_colors['sidebar_bg'], fg=self.app_colors['fg'])
                btn.bind('<Enter>', on_enter)
                btn.bind('<Leave>', on_leave)
                if self.selected_menu == txt:
                    btn.config(bg=self.app_colors['accent'], fg=self.app_colors['sidebar_bg'])
            else:
                if img:
                    btn = tk.Label(
                        self.sidebar_frame,
                        image=img,
                        bg=self.app_colors['sidebar_bg'],
                        padx=8,
                        pady=8,
                    )
                else:
                    btn = tk.Button(
                        self.sidebar_frame,
                        text=icon,
                        font=('Segoe UI', 16),
                        bg=self.app_colors['sidebar_bg'],
                        fg=self.app_colors['accent'],
                        bd=0,
                        command=make_cmd(txt),
                        relief='flat',
                        activebackground=self.app_colors['bg'],
                        activeforeground=self.app_colors['highlight']
                    )
                btn.pack(pady=10, padx=0)
        # Add copyright label at the bottom only if sidebar is expanded
        if self.sidebar_expanded.get():
            copyright_lbl = tk.Label(
                self.sidebar_frame,
                text='© 2025 RCPA Team.\nAll rights reserved.',
                font=('Segoe UI', 9, 'italic'),
                bg=self.app_colors['sidebar_bg'],
                fg='#c0c0c0',
                anchor='s',
                justify='center'
            )
            copyright_lbl.pack(side='bottom', pady=(0, 8), fill='x')

    def _select_menu(self, page):
        self.selected_menu = page
        self._update_sidebar()
        self._show_page(page)

    def _show_page(self, page):
        for widget in self.content_frame.winfo_children():
            widget.destroy()
        if page == 'Home':
            self._build_home_page(self.content_frame)
        elif page == 'Report':
            self._build_report_page(self.content_frame)
        elif page == 'Settings':
            self._build_settings_page(self.content_frame)
        # Removed Help page

    def _build_home_page(self, parent):
        # Split into 2 sections: left (40%) config, right (60%) validation/logs
        main_frame = tk.Frame(parent, bg=self.app_colors['bg'])
        main_frame.pack(fill='both', expand=True)
        main_frame.grid_columnconfigure(0, weight=2)
        main_frame.grid_columnconfigure(1, weight=3)
        # Page title
        tk.Label(main_frame, text='SQL Server To PostgreSQL Data Validation', font=('Segoe UI', 14, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent']).grid(row=0, column=0, columnspan=2, sticky='w', padx=16, pady=(10, 6))
        # Left: Config Section (40%)
        config_frame = tk.Frame(main_frame, bg=self.app_colors['bg'], highlightbackground='#232323', highlightthickness=2)
        config_frame.grid(row=1, column=0, sticky='nsew', padx=(16, 4), pady=(0, 8))  # Reduce right gap
        config_frame.grid_columnconfigure(0, weight=1)
        config_frame.grid_columnconfigure(1, weight=1)
        # --- SQL/PG Connection Sections side by side ---
        sql_conn_frame = tk.LabelFrame(config_frame, text='SQL Server Connection', font=('Segoe UI', 10, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent'], relief='groove', bd=2)
        sql_conn_frame.grid(row=0, column=0, sticky='nsew', padx=(8, 2), pady=(8, 4))  # Reduce right gap
        pg_conn_frame = tk.LabelFrame(config_frame, text='PostgreSQL Connection', font=('Segoe UI', 10, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent'], relief='groove', bd=2)
        pg_conn_frame.grid(row=0, column=1, sticky='nsew', padx=(2, 8), pady=(8, 4))  # Reduce left gap
        config_frame.grid_rowconfigure(0, weight=1)
        # SQL controls (remove DB field)
        sql_labels = [('Server:', self.sql_host), ('User:', self.sql_user), ('Pwd:', self.sql_pwd), ('Driver:', self.sql_driver)]
        for i, (label, var) in enumerate(sql_labels):
            tk.Label(sql_conn_frame, text=label, font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=i, column=0, sticky='e', padx=(4, 2), pady=2)
            entry_opts = {'font': ('Segoe UI', 9), 'width': 18, 'relief': 'groove', 'bd': 1, 'highlightthickness': 1, 'bg': '#f5f5f5'}
            entry = tk.Entry(sql_conn_frame, textvariable=var, **entry_opts)
            entry.grid(row=i, column=1, sticky='w', padx=(2, 8), pady=2, ipady=2)
            entry.config(highlightbackground='#232323', highlightcolor='#232323')
        # Win Auth checkbox and Test button in same row, grouped together
        tk.Label(sql_conn_frame, text='Win Auth:', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=len(sql_labels), column=0, sticky='e', padx=(4, 2), pady=2)
        win_auth_frame = tk.Frame(sql_conn_frame, bg=self.app_colors['bg'])
        win_auth_frame.grid(row=len(sql_labels), column=1, sticky='w', padx=(2, 8), pady=2)
        tk.Checkbutton(win_auth_frame, variable=self.sql_win_auth, bg=self.app_colors['bg'], fg=self.app_colors['fg'], selectcolor=self.app_colors['bg'], font=('Segoe UI', 9), relief='flat', bd=0, highlightthickness=0).pack(side='left', padx=(0, 4))
        tk.Button(win_auth_frame, text='⚡Test', font=('Segoe UI', 9, 'bold'), bg=self.app_colors['accent'], fg='#000000', command=self._test_sql_conn, relief='groove', bd=1, padx=8, pady=2).pack(side='left')
        # PG controls (remove DB field)
        pg_labels = [('Server:', self.pg_host), ('User:', self.pg_user), ('Pwd:', self.pg_pwd), ('Port:', self.pg_port)]
        for i, (label, var) in enumerate(pg_labels):
            tk.Label(pg_conn_frame, text=label, font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=i, column=0, sticky='e', padx=(4, 2), pady=2)
            entry_opts = {'font': ('Segoe UI', 9), 'width': 18, 'relief': 'groove', 'bd': 1, 'highlightthickness': 1, 'bg': '#f5f5f5'}
            entry = tk.Entry(pg_conn_frame, textvariable=var, **entry_opts)
            entry.grid(row=i, column=1, sticky='w', padx=(2, 8), pady=2, ipady=2)
            entry.config(highlightbackground='#232323', highlightcolor='#232323')
        tk.Button(pg_conn_frame, text='⚡Test', font=('Segoe UI', 9, 'bold'), bg=self.app_colors['accent'], fg='#000000', command=self._test_pg_conn, relief='groove', bd=1, padx=8, pady=2).grid(row=len(pg_labels), column=0, columnspan=2, sticky='e', padx=(2, 8), pady=4)
        # --- Horizontal SQL DB, PG DB, Batch Size ---
        db_batch_frame = tk.Frame(config_frame, bg=self.app_colors['bg'])
        db_batch_frame.grid(row=1, column=0, columnspan=2, sticky='ew', padx=8, pady=(4, 2))
        # SQL DB
        tk.Label(db_batch_frame, text='SQL DB:', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=0, column=0, sticky='e', padx=(4, 2), pady=2)
        self.sql_db_dropdown = ttk.Combobox(db_batch_frame, state='disabled', width=14)
        self.sql_db_dropdown.grid(row=0, column=1, sticky='w', padx=(2, 8), pady=2)
        self.sql_db_dropdown.bind('<<ComboboxSelected>>', self._reload_sql_tables)
        # PG DB
        tk.Label(db_batch_frame, text='PG DB:', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=0, column=2, sticky='e', padx=(4, 2), pady=2)
        self.pg_db_dropdown = ttk.Combobox(db_batch_frame, state='disabled', width=14)
        self.pg_db_dropdown.grid(row=0, column=3, sticky='w', padx=(2, 8), pady=2)
        # Batch Size
        tk.Label(db_batch_frame, text='Batch Size:', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=0, column=4, sticky='e', padx=(4, 2), pady=2)
        self.batch_size = tk.StringVar(value='1000')
        tk.Entry(db_batch_frame, textvariable=self.batch_size, font=('Segoe UI', 9), width=10, relief='groove', bd=1, highlightthickness=1, bg='#f5f5f5').grid(row=0, column=5, sticky='w', padx=(2, 8), pady=2)
        # Output Format (CSV/Table Store)
        tk.Label(db_batch_frame, text='Output Format:', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['fg']).grid(row=0, column=6, sticky='e', padx=(4, 2), pady=2)
        self.output_type = tk.StringVar(value='CSV')
        self.output_type_dropdown = ttk.Combobox(db_batch_frame, textvariable=self.output_type, values=['CSV', 'Table Store'], state='readonly', width=12)
        self.output_type_dropdown.grid(row=0, column=7, sticky='w', padx=(2, 8), pady=2)
        # --- Table Selection (Treeview with checkboxes) ---
        table_frame = tk.Frame(config_frame, bg=self.app_colors['bg'])
        table_frame.grid(row=2, column=0, columnspan=2, sticky='ew', padx=8, pady=(2, 2))
        # Instruction label (left)
        instr_lbl = tk.Label(table_frame, text="After selecting tables, click 'Update Table' to reflect in validation process.", font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['accent'])
        instr_lbl.grid(row=0, column=0, sticky='w', padx=(4, 2), pady=2)
        # Update Table button (right)
        update_btn = tk.Button(table_frame, text='Update Table', font=('Segoe UI', 9, 'bold'), bg=self.app_colors['accent'], fg='#000000', command=self._update_table_selection, relief='groove', bd=1, padx=8, pady=2)
        update_btn.grid(row=0, column=1, sticky='e', padx=(2, 8), pady=2)
        # Treeview with checkboxes
        self.table_tree = ttk.Treeview(table_frame, columns=('Table',), show='tree', selectmode='none', height=8)
        self.table_tree.grid(row=1, column=0, columnspan=2, sticky='nsew', padx=(2, 0), pady=(0, 2))
        table_frame.grid_rowconfigure(1, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        # Add vertical scrollbar
        tree_scroll = ttk.Scrollbar(table_frame, orient='vertical', command=self.table_tree.yview)
        tree_scroll.grid(row=1, column=2, sticky='ns')
        self.table_tree.configure(yscrollcommand=tree_scroll.set)
        # Checkbox characters for Treeview text
        self._checkbox_checked = '\u2611'  # ☑
        self._checkbox_unchecked = '\u2610'  # ☐
        self.table_tree.tag_configure('checked', foreground=self.app_colors['fg'])
        self._table_checks = {}
        self.tables_updated = False
        self.table_tree.bind('<Button-1>', self._on_table_tree_click)
        # --- Validation Buttons ---
        btn_frame = tk.Frame(config_frame, bg=self.app_colors['bg'])
        btn_frame.grid(row=3, column=0, columnspan=2, sticky='ew', padx=8, pady=(2, 8))
        btn_frame.grid_columnconfigure(0, weight=1)
        # Create a horizontal sub-frame to group Start and Cancel buttons
        btns_inner_frame = tk.Frame(btn_frame, bg=self.app_colors['bg'])
        btns_inner_frame.pack(anchor='center')
        self.start_btn = tk.Button(
            btns_inner_frame,
            text='Start Validation 🚀',
            font=('Segoe UI', 10, 'bold'),
            bg=self.app_colors['accent'],
            fg='#000000',
            command=self._start_validation,
            relief='groove',
            bd=1,
            padx=16,
            pady=4,
            width=16,
            state='normal'
        )
        self.start_btn.pack(side='left', padx=(32, 8), pady=2)
        self.stop_btn = tk.Button(btns_inner_frame, text='Cancel', font=('Segoe UI', 9, 'bold'), bg='#ff4444', fg='#ffffff', command=self._stop_validation, relief='groove', bd=1, padx=12, pady=4)
        self.stop_btn.pack(side='left', padx=(0, 8), pady=2)
        self.stop_btn.pack_forget()  # Hide abort/cancel button initially
        # --- Validation/Log Section (right, 60%) ---
        right_frame = tk.Frame(main_frame, bg=self.app_colors['bg'], highlightbackground='#232323', highlightthickness=2)
        right_frame.grid(row=1, column=1, sticky='nsew', padx=(4, 16), pady=(0, 8))
        right_frame.grid_columnconfigure(0, weight=1)
        # Progress Bar
        self.progress_label = tk.Label(right_frame, text='Validation Progress: 0 / 0 rows (0%)', font=('Segoe UI', 11, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent'])
        self.progress_label.grid(row=0, column=0, sticky='ew', padx=8, pady=(12, 2))
        self.progress_bar = ttk.Progressbar(right_frame, orient='horizontal', mode='determinate', length=400, style="green.Horizontal.TProgressbar")
        self.progress_bar.grid(row=1, column=0, sticky='ew', padx=8, pady=(2, 12))
        style = ttk.Style()
        style.theme_use('clam')
        style.configure("green.Horizontal.TProgressbar", foreground='#006400', background='#006400')
        right_frame.grid_rowconfigure(1, weight=0)
        # Log Text (ScrolledText)
        log_label = tk.Label(right_frame, text='Log', font=('Segoe UI', 11, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent'])
        log_label.grid(row=2, column=0, sticky='w', padx=8, pady=(8, 2))
        self.log_text_widget = ScrolledText(right_frame, wrap='word', height=20, width=60, font=('Segoe UI', 10), bg='#232323', fg='white')
        self.log_text_widget.grid(row=3, column=0, sticky='nsew', padx=10, pady=10)
        right_frame.grid_rowconfigure(3, weight=1)
        # Configure tags for color
        self.log_text_widget.tag_config('debug', foreground='gray')
        self.log_text_widget.tag_config('error', foreground='red')
        self.log_text_widget.tag_config('warning', foreground='orange')
        self.log_text_widget.tag_config('info', foreground='white')
        self._refresh_home_log_text()
        # Global notification label (hidden by default)
        # REMOVED: self.global_notification = tk.Label(parent, text='', bg=self.app_colors['bg'], fg='#ffffff', font=('Segoe UI', 9), anchor='w')
        # self.global_notification.place_forget()

    def _refresh_home_log_text(self):
        # Only update log widget if it exists and is mapped (visible)
        if self.log_text_widget and self.log_text_widget.winfo_exists() and self.log_text_widget.winfo_ismapped():
            self.log_text_widget.config(state='normal')
            self.log_text_widget.delete(1.0, 'end')
            # Show only table-wise log entries (skip row-by-row logs)
            minimal_logs = []
            for entry in self.global_log[-100:]:
                # Only include logs that mention table-level events
                if 'table' in entry.lower() or 'tables' in entry.lower() or 'summary' in entry.lower() or 'validation' in entry.lower():
                    minimal_logs.append(entry)
            for entry in minimal_logs:
                # Extract log level for color tag
                if entry.startswith('['):
                    level = entry.split(']', 1)[0][1:].lower()
                else:
                    level = 'info'
                self.log_text_widget.insert('end', entry + '\n', level)
            self.log_text_widget.see('end')
        # Do not disable so log updates dynamically

    def log(self, msg, success=True, level='INFO'):
        entry = f"[{level}] {msg}"
        self.global_log.append(entry)
        # Only refresh log widget if it exists and is mapped
        if self.log_text_widget and self.log_text_widget.winfo_exists() and self.log_text_widget.winfo_ismapped():
            self._refresh_home_log_text()
        # Show notification only once when validation is complete
        if level == 'COMPLETE':
            self.show_global_notification(entry, success=True)
        # Show warnings/errors in global notification except for validation process
        elif level in ('WARNING', 'ERROR'):
            if not ('validation' in msg.lower() or 'validation progress' in msg.lower()):
                self.show_global_notification(entry, success=(level != 'ERROR'))

    def _start_validation(self):
        # Clear stop_event before starting validation to allow re-run
        self.stop_event.clear()
        # Check prerequisites and warn if missing
        sql_ok = self.sql_db_dropdown.get().strip()
        pg_ok = self.pg_db_dropdown.get().strip()
        tables_ok = bool(self.config.tables)
        if not (sql_ok and pg_ok and tables_ok):
            self.log("[WARNING] Please select SQL DB, PG DB, and update table selection before starting validation.", success=False)
            return
        # --- Set config db fields from dropdowns at validation time ---
        self.config.sqlserver['db'] = sql_ok
        self.config.postgres['db'] = pg_ok
        self.start_btn['state'] = 'disabled'
        self.stop_btn.pack()  # Show cancel/abort button
        self.progress_bar['value'] = 0
        self.progress_label['text'] = 'Validation Progress: 0 / 0 rows'
        tables = self.config.tables
        if not tables:
            self.log('[ERROR] No tables selected for validation.', success=False)
            self.start_btn['state'] = 'normal'
            self.stop_btn.pack_forget()
            return
        batch_size = int(self.batch_size.get())
        output_mode = self.output_type.get()
        config_dict = {
            'batch_size': batch_size,
            'output_mode': output_mode,
            'output_path': self.config.output_path if hasattr(self.config, 'output_path') else './mismatches'
        }
        self.log(f"[INFO] Starting validation for tables: {tables}", success=True)
        # --- Sanitize host/user fields before building connection string ---
        host_val = self.pg_host.get().strip()
        if '@' in host_val:
            corrected_host = host_val.split('@')[-1].strip()
            self.pg_host.set(corrected_host)
            host_val = corrected_host
        user_val = self.pg_user.get().strip()
        if '@' in user_val:
            sanitized_user = user_val.split('@')[0].strip()
            self.pg_user.set(sanitized_user)
            user_val = sanitized_user
            self.log(f"User field corrected to '{sanitized_user}' (removed @host)", success=False)
        self.config.postgres['server'] = host_val
        self.config.postgres['user'] = user_val
        self.config.postgres['pwd'] = self.pg_pwd.get().strip()
        self.config.postgres['port'] = self.pg_port.get().strip()
        self.config.save()
        sql_conn_str = self.config.build_sql_conn_str()
        pg_conn_str = self.config.build_pg_conn_str()
        self.log(f"[DEBUG] PG Host: '{host_val}' | PG User: '{user_val}' | PG DB: '{self.pg_db.get().strip()}' | PG Port: '{self.pg_port.get().strip()}' | PG Pwd: '{self.pg_pwd.get().strip()}'")
        # --- Ensure schema table exists before validation ---
        def ensure_schema_table_exists():
            """Always create schema_mismatches table in PG public schema before validation."""
            try:
                import sqlalchemy
                pg_conn_str = self.config.build_pg_conn_str()
                db_name = self.config.postgres.get('db', '')
                self.log(f"Ensuring table public.schema_mismatches in database '{db_name}' using connection: {pg_conn_str}", success=True)
                engine = sqlalchemy.create_engine(pg_conn_str)
                with engine.connect() as conn:
                    conn.execute(sqlalchemy.text("""
                        CREATE TABLE IF NOT EXISTS public.schema_mismatches (
                            id SERIAL PRIMARY KEY,
                            table_name TEXT NOT NULL,
                            column_name TEXT NOT NULL,
                            mismatch_type TEXT NOT NULL,
                            details JSONB,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    self.log(f"Table public.schema_mismatches ensured in database '{db_name}'.", success=True)
            except Exception as e:
                self.log(f"Error creating schema table: {e}", success=False)
                self.start_btn['state'] = 'normal'
                self.stop_btn.pack_forget()
                return False
            return True
        def run_validation():
            if not ensure_schema_table_exists():
                return
            engine = ValidationEngine(sql_conn_str, pg_conn_str, config_path=self.config.config_path, ui_log_callback=self.log)
            engine.batch_size = batch_size
            engine.output_mode = output_mode
            engine.stop_event = self.stop_event
            total_rows = None
            def update_progress(processed_tables, processed_rows, table_rows=None):
                nonlocal total_rows
                if total_rows is None:
                    total_rows = engine.summary.get('total_rows_estimated', 1)
                percent = int((processed_rows / max(total_rows, 1)) * 100)
                self.progress_bar['maximum'] = total_rows
                self.progress_bar['value'] = processed_rows
                self.progress_label['text'] = f"Validation Progress: {processed_rows} / {total_rows} rows ({percent}%)"
                self.update_idletasks()
                if self.stop_event.is_set():
                    self.log('[DEBUG] Cancellation detected in UI progress callback. Exiting validation thread.', success=False)
                    self.progress_label['text'] = 'Validation cancelled.'
                    self.start_btn['state'] = 'normal'
                    self.stop_btn.pack_forget()
                    raise SystemExit
            try:
                if self.stop_event.is_set():
                    self.log('[DEBUG] Cancellation detected before validation start. Exiting validation thread.', success=False)
                    self.progress_label['text'] = 'Validation cancelled.'
                    self.start_btn['state'] = 'normal'
                    self.stop_btn.pack_forget()
                    return
                summary = engine.run_all(tables, progress_callback=update_progress)
                if self.stop_event.is_set():
                    self.log('[DEBUG] Cancellation detected after engine.run_all. Exiting validation thread.', success=False)
                    self.progress_label['text'] = 'Validation cancelled.'
                    self.start_btn['state'] = 'normal'
                    self.stop_btn.pack_forget()
                    return
                if summary:
                    final_msg = (
                        f"Total Tables: {summary['tables_validated']} | "
                        f"Total Rows Processed: {summary.get('total_rows_estimated', 0)} | "
                        f"Total Mismatched: {summary['mismatched_rows']} | "
                        f"Duration: {summary['duration']}"
                    )
                    self.log(final_msg, success=True)
                    self.progress_label['text'] = final_msg
                    self.show_global_notification("Validation process completed.", success=True)
                else:
                    self.log("[ERROR] Validation aborted: No valid tables to process.", success=False)
                    self.show_global_notification("Validation aborted: No valid tables to process.", success=False)
                self.start_btn['state'] = 'normal'
                self.stop_btn.pack_forget()
            except SystemExit:
                self.log('[DEBUG] Validation thread exited due to cancellation.', success=False)
                self.progress_label['text'] = 'Validation cancelled.'
                self.start_btn['state'] = 'normal'
                self.stop_btn.pack_forget()
                self.show_global_notification('Validation cancelled.', success=False)
                return
        threading.Thread(target=run_validation, daemon=True).start()

    def _stop_validation(self):
        self.log('[DEBUG] Cancel button pressed. Setting stop_event.')
        self.stop_event.set()
        self.start_btn['state'] = 'normal'
        self.stop_btn.pack_forget()

    def _export_summary(self):
        # Dummy: export summary
        self.log('Exporting summary...')
        filedialog.asksaveasfilename(title='Export Summary', filetypes=[('CSV Files', '*.csv')])

    def process_ui_queue(self):
        while not self.ui_queue.empty():
            msg = self.ui_queue.get()
            if msg['type'] == 'log':
                self.log(msg['text'])
            elif msg['type'] == 'progress':
                self.progress_bar['value'] = msg.get('progress', 0)
            elif msg['type'] == 'status':
                table = msg['table']
                compared = msg['compared']
                found = False
                for iid in self.status_tree.get_children():
                    vals = self.status_tree.item(iid)['values']
                    if vals and vals[0] == table:
                        self.status_tree.item(iid, values=(table, compared, mismatches, status))
                        found = True
                        break
                if not found:
                    self.status_tree.insert('', 'end', values=(table, compared, mismatches, status))
            elif msg['type'] == 'summary':
                self.summary_label['text'] = msg['text']
        self.after(100, self.process_ui_queue)

    def show_global_notification(self, message, success=True, duration=3500):
        if success:
            icon = '\u2714'  # checkmark
            color = '#ffffff'  # white for success
        else:
            icon = '\u26A0'  # warning
            color = '#c00000'  # red
        self.global_notification.config(text=f'{icon} {message}',
            fg=color,
            bg=self.app_colors['bg'],
            font=('Segoe UI', 9),
            anchor='w')
        self.global_notification.lift()
        self.global_notification.place_configure(relx=0.5, rely=0.0, anchor='n', y=12, relwidth=0.35)
        self.after(duration, lambda: self.global_notification.lower())

    def _get_selected_tables(self):
        # Return checked tables from treeview (schema-qualified)
        selected = []
        for iid, checked in self._table_checks.items():
            if checked:
                orig_text = self.table_tree.item(iid, 'text')
                tbl_name = orig_text.split('  ', 1)[-1]
                selected.append(tbl_name)
        return selected

    def _load_sql_tables(self):
        # Fetch tables from selected SQL Server DB and populate treeview
        self.log('Loading tables from SQL Server...')
        import sqlalchemy
        conn_str = self.config.build_sql_conn_str()
        try:
            engine = sqlalchemy.create_engine(conn_str)
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"))
                table_names = [row[0] for row in result]
            # Clear treeview
            for iid in self.table_tree.get_children():
                self.table_tree.delete(iid)
            self._table_checks.clear()
            for tbl in table_names:
                iid = self.table_tree.insert('', 'end', text=tbl, image=self._img_checked)
                self._table_checks[iid] = True
            self.log(f"Loaded {len(table_names)} tables.", success=True)
        except Exception as e:
            self.log(f"Error loading tables: {e}", success=False)

    def _reload_sql_tables(self, event=None):
        # Fetch tables from selected SQL Server DB and populate treeview
        self.log('Loading tables from SQL Server...')
        import sqlalchemy
        db_val = self.sql_db_dropdown.get().strip()
        if not db_val:
            self.log('No SQL database selected.', success=False)
            return
        self.config.sqlserver['db'] = db_val
        self.config.save()
        conn_str = self.config.build_sql_conn_str()
        try:
            engine = sqlalchemy.create_engine(conn_str)
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"))
                table_names = [f"{row[0]}.{row[1]}" for row in result]
            # Clear treeview
            for iid in self.table_tree.get_children():
                self.table_tree.delete(iid)
            self._table_checks.clear()
            for tbl in table_names:
                display_text = f"{self._checkbox_checked}  {tbl}"
                iid = self.table_tree.insert('', 'end', text=display_text, tags=('checked',))
                self._table_checks[iid] = True
            # After reload, require user to click Update Table again
        except Exception as e:
            self.log(f"Error loading tables: {e}", success=False)

    def _on_table_tree_click(self, event):
        x, y = event.x, event.y
        iid = self.table_tree.identify_row(y)
        region = self.table_tree.identify('region', x, y)
        if not iid or region not in ('tree', 'cell'):
            return
        # Toggle checked state
        checked = self._table_checks.get(iid, True)
        new_state = not checked
        self._table_checks[iid] = new_state
        # Update checkbox character in text
        orig_text = self.table_tree.item(iid, 'text')
        tbl_name = orig_text.split('  ', 1)[-1]
        new_text = f"{self._checkbox_checked if new_state else self._checkbox_unchecked}  {tbl_name}"
        self.table_tree.item(iid, text=new_text)
        if new_state:
            self.table_tree.item(iid, tags=('checked',))
        else:
            self.table_tree.item(iid, tags=())
        # After any checkbox change, require Update Table
        return 'break'

    def _update_table_selection(self):
        # Update config tables array with checked tables (schema-qualified)
        selected_tables = self._get_selected_tables()
        self.config.tables = selected_tables
        self.config.save(log_tables=True)
        self.tables_updated = True  # Always set to True after update
        self.log(f"[INFO] Table selection updated. Ready for validation.", success=True)
        self.show_global_notification("Table selection updated. Ready for validation.", success=True)

    def _sync_schema_table(self):
        # Ensure the schema table exists in the connected database
        self.log('Syncing schema table...')
        # --- Determine target database connection string ---
        target_db = self.pg_db_dropdown.get().strip()
        if not target_db:
            self.log('No target database selected for schema sync.', success=False)
            return
        self.config.postgres['db'] = target_db
        self.config.save()
        pg_conn_str = self.config.build_pg_conn_str()
        # --- Schema table creation logic ---
        try:
            import sqlalchemy
            engine = sqlalchemy.create_engine(pg_conn_str)
            with engine.connect() as conn:
                # Check if schema table exists
                result = conn.execute(sqlalchemy.text("SELECT to_regclass('public.schema_mismatches')"))
                exists = result.scalar() is not None
                if not exists:
                    # Create schema table if it doesn't exist
                    conn.execute(sqlalchemy.text("""
                        CREATE TABLE public.schema_mismatches (
                            id SERIAL PRIMARY KEY,
                            table_name TEXT NOT NULL,
                            column_name TEXT NOT NULL,
                            mismatch_type TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """))
                    self.log('Schema table created successfully.', success=True)
                else:
                    self.log('Schema table already exists.', success=True)
        except Exception as e:
            self.log(f"Error syncing schema table: {e}", success=False)

    def _test_sql_conn(self):
        self.log('Testing SQL Server connection...')
        # Update config from UI
        self.config.sqlserver['server'] = self.sql_host.get()
        self.config.sqlserver['db'] = self.sql_db.get()
        self.config.sqlserver['user'] = self.sql_user.get()
        self.config.sqlserver['pwd'] = self.sql_pwd.get()
        self.config.sqlserver['driver'] = self.sql_driver.get()
        self.config.sqlserver['win_auth'] = self.sql_win_auth.get()
        self.config.save()
        ok, msg = self.config.test_sql_connection()
        self.log(msg)
        self.show_global_notification(msg, success=ok)
        if ok:
            # Fetch DB list using SQLAlchemy
            try:
                import sqlalchemy
                engine = sqlalchemy.create_engine(self.config.build_sql_conn_str())
                with engine.connect() as conn:
                    result = conn.execute(sqlalchemy.text("SELECT name FROM sys.databases"))
                    self.sql_db_list = [row[0] for row in result]
                self.sql_db_dropdown['values'] = self.sql_db_list
                self.sql_db_dropdown['state'] = 'readonly'
                self.sql_db_dropdown.set(self.sql_db.get() or self.sql_db_list[0])
                self._reload_sql_tables()
            except Exception as e:
                self.log(f"Error fetching SQL DB list: {e}")
                self.sql_db_dropdown['state'] = 'disabled'
        else:
            self.sql_db_dropdown['state'] = 'disabled'

    def _test_pg_conn(self):
        self.log('Testing PostgreSQL connection...')
        # Always use current UI field values and sanitize host
        host_val = self.pg_host.get().strip()
        if '@' in host_val:
            corrected_host = host_val.split('@')[-1].strip()
            self.pg_host.set(corrected_host)
            self.show_global_notification(f"Host field corrected to '{corrected_host}' (removed user@)", success=False)
            host_val = corrected_host
        db_val = self.pg_db.get().strip()
        user_val = self.pg_user.get().strip()
        pwd_val = self.pg_pwd.get().strip()
        port_val = self.pg_port.get().strip()
        # Log the actual host value used
        self.log(f"Using PostgreSQL host: '{host_val}'", success=True)
        # --- Log all connection parameters for debugging ---
        self.log(f"[DEBUG] PG Host: '{host_val}' | PG User: '{user_val}' | PG DB: '{db_val}' | PG Port: '{port_val}' | PG Pwd: '{pwd_val}'")
        # Prevent connection if host is still invalid
        if '@' in host_val or not host_val:
            self.log("Invalid PostgreSQL host value. Please check the host field.", success=False)
            self.pg_db_dropdown['state'] = 'disabled'
            return
        # Update config for persistence
        self.config.postgres['server'] = host_val
        self.config.postgres['db'] = db_val
        self.config.postgres['user'] = user_val
        self.config.postgres['pwd'] = pwd_val
        self.config.postgres['port'] = port_val
        self.config.save()
        # Build connection string using config method (handles URL-encoding)
        import sqlalchemy
        conn_str = self.config.build_pg_conn_str()
        # Log the actual connection string for debugging
        self.log(f"[DEBUG] Connection string: {conn_str}")
        # Test connection
        try:
            engine = sqlalchemy.create_engine(conn_str)
            with engine.connect() as conn:
                pass
            self.pg_db_dropdown['values'] = self.pg_db_list
            self.pg_db_dropdown['state'] = 'readonly'
            self.pg_db_dropdown.set(db_val or (self.pg_db_list[0] if self.pg_db_list else ''))
            self.log('PostgreSQL connection succeeded.', success=True)
            self.show_global_notification('PostgreSQL connection succeeded.', success=True)
        except Exception as e:
            self.log(f"PostgreSQL connection error: {e}", success=False)
            self.show_global_notification(f"PostgreSQL connection error: {e}", success=False)
            self.pg_db_dropdown['state'] = 'disabled'
        # Removed call to _enable_start_btn_if_ready

    def _build_settings_page(self, parent):
        # Settings page: output report/log directory selection
        frame = tk.Frame(parent, bg=self.app_colors['bg'])
        frame.pack(fill='both', expand=True)
        tk.Label(frame, text='Application Settings', font=('Segoe UI', 14, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent']).pack(anchor='w', padx=16, pady=(16, 8))
        # Output Report Directory
        dir_frame = tk.Frame(frame, bg=self.app_colors['bg'])
        dir_frame.pack(anchor='w', padx=32, pady=(8, 4))
        tk.Label(dir_frame, text='Output Report Directory:', font=('Segoe UI', 11), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left')
        self.output_dir_var = tk.StringVar(value=getattr(self.config, 'output_path', './ValidationReports'))
        dir_entry = tk.Entry(dir_frame, textvariable=self.output_dir_var, font=('Segoe UI', 10), width=40, relief='groove', bd=1, highlightthickness=1, bg='#f5f5f5')
        dir_entry.pack(side='left', padx=(8, 4))
        def browse_dir():
            path = filedialog.askdirectory(title='Select Output Report Directory')
            if path:
                self.output_dir_var.set(path)
                self.config.output_path = path
                self.config.log_path = os.path.join(path, 'Logs')
                self.config.save()
                self.log(f"[INFO] Output report directory set to: {path}", success=True)
        tk.Button(dir_frame, text='Browse', font=('Segoe UI', 10), bg=self.app_colors['accent'], fg='#000000', command=browse_dir, relief='groove', bd=1, padx=8, pady=2).pack(side='left', padx=(4, 0))
        # Info label
        tk.Label(frame, text='All logs and validation reports will be generated in this directory.', font=('Segoe UI', 9), bg=self.app_colors['bg'], fg=self.app_colors['accent']).pack(anchor='w', padx=32, pady=(2, 8))
        # Save button
        def save_settings():
            path = self.output_dir_var.get().strip()
            if path:
                self.config.output_path = path
                self.config.log_path = os.path.join(path, 'Logs')
                self.config.save()
                self.log(f"[INFO] Settings saved. Output report directory: {path}", success=True)
                self.show_global_notification(f"Settings saved! Output directory: {path}", success=True, duration=2500)
        tk.Button(frame, text='Save Settings', font=('Segoe UI', 10, 'bold'), bg=self.app_colors['accent'], fg='#000000', command=save_settings, relief='groove', bd=1, padx=12, pady=4).pack(anchor='w', padx=32, pady=(8, 0))
    def _build_report_page(self, parent):
        frame = tk.Frame(parent, bg=self.app_colors['bg'])
        frame.pack(fill='both', expand=True)
        # --- Global DB dropdown ---
        db_top = tk.Frame(frame, bg=self.app_colors['bg'])
        db_top.pack(fill='x', pady=(8, 0), padx=16)
        tk.Label(db_top, text='Select Database:', font=('Segoe UI', 11, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent']).pack(side='left')
        self.report_db_var = tk.StringVar()
        self.report_db_dropdown = ttk.Combobox(db_top, textvariable=self.report_db_var, state='readonly', width=24)
        self.report_db_dropdown.pack(side='left', padx=(8, 0))
        self.report_db_dropdown.bind('<<ComboboxSelected>>', lambda e: self._on_report_db_change())
        # Populate DB dropdown
        try:
            pg_conn_str = self.config.build_pg_conn_str()
            engine = sqlalchemy.create_engine(pg_conn_str)
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("SELECT datname FROM pg_database WHERE datistemplate = false"))
                dbs = [row[0] for row in result]
            self.report_db_dropdown['values'] = dbs
            self.report_db_var.set(self.config.postgres.get('db', dbs[0] if dbs else ''))
        except Exception as e:
            self.report_db_dropdown['values'] = []
            self.report_db_var.set('')
            self.log(f"[ERROR] Could not load database list: {e}", success=False, level='ERROR')
        # --- Top/Bottom Layout ---
        # Summary (top)
        summary_frame = tk.Frame(frame, bg=self.app_colors['bg'])
        summary_frame.pack(fill='both', expand=True, padx=16, pady=(12, 4))
        filter_frame = tk.Frame(summary_frame, bg=self.app_colors['bg'])
        filter_frame.pack(fill='x', pady=(0, 6))
        tk.Label(filter_frame, text='Summary Records', font=('Segoe UI', 12, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent']).pack(side='left')
        self.summary_total_var = tk.StringVar(value='Total: 0')
        tk.Label(filter_frame, textvariable=self.summary_total_var, font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left', padx=(12, 0))
        tk.Label(filter_frame, text='Table:', font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left', padx=(16, 2))
        self.summary_search_var = tk.StringVar()
        search_entry = tk.Entry(filter_frame, textvariable=self.summary_search_var, font=('Segoe UI', 10), width=16)
        search_entry.pack(side='left', padx=(0, 8))
        tk.Label(filter_frame, text='Status:', font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left', padx=(4, 2))
        self.summary_status_var = tk.StringVar(value='All')
        status_combo = ttk.Combobox(filter_frame, textvariable=self.summary_status_var, values=['All', 'Matched', 'Mismatched'], state='readonly', width=12)
        status_combo.pack(side='left', padx=(0, 8))
        tk.Button(filter_frame, text='Filter', font=('Segoe UI', 9), command=self._refresh_summary_grid).pack(side='left')
        self.summary_columns = ['pk_summary_id', 'table_name', 'total_rows', 'mismatched', 'validation_timestamp', 'status']
        col_titles = ['ID', 'Table', 'Total Rows', 'Mismatched', 'Validated At', 'Status']
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('Custom.Treeview.Heading', background=self.app_colors['accent'], foreground='#000000', font=('Segoe UI', 10, 'bold'))
        self.summary_tree = ttk.Treeview(summary_frame, columns=self.summary_columns, show='headings', height=12, style='Custom.Treeview')
        for col, title in zip(self.summary_columns, col_titles):
            self.summary_tree.heading(col, text=title)
            self.summary_tree.column(col, width=120, anchor='w')
        self.summary_tree.pack(fill='both', expand=True)
        self.summary_tree.bind('<<TreeviewSelect>>', self._on_summary_row_select)
        # Pagination controls for summary grid
        pag_frame = tk.Frame(summary_frame, bg=self.app_colors['bg'])
        pag_frame.pack(fill='x', pady=(4, 0))
        self.summary_page_var = tk.IntVar(value=1)
        self.summary_page_size = 25
        self.summary_max_page = 1
        tk.Button(pag_frame, text='Prev', command=self._summary_prev_page).pack(side='left', padx=4)
        self.summary_page_label = tk.Label(pag_frame, text='Page 1/1', font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg'])
        self.summary_page_label.pack(side='left', padx=8)
        tk.Button(pag_frame, text='Next', command=self._summary_next_page).pack(side='left', padx=4)
        # Details (bottom)
        details_frame = tk.Frame(frame, bg=self.app_colors['bg'])
        details_frame.pack(fill='both', expand=True, padx=16, pady=(4, 12))
        details_top = tk.Frame(details_frame, bg=self.app_colors['bg'])
        details_top.pack(fill='x', pady=(0, 6))
        tk.Label(details_top, text='Details', font=('Segoe UI', 12, 'bold'), bg=self.app_colors['bg'], fg=self.app_colors['accent']).pack(side='left')
        self.details_total_var = tk.StringVar(value='Total: 0')
        tk.Label(details_top, textvariable=self.details_total_var, font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left', padx=(12, 0))
        tk.Label(details_top, text='Filter:', font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg']).pack(side='left', padx=(16, 2))
        self.details_filter_var = tk.StringVar()
        details_search_entry = tk.Entry(details_top, textvariable=self.details_filter_var, font=('Segoe UI', 10), width=16)
        details_search_entry.pack(side='left', padx=(0, 8))
        tk.Button(details_top, text='Filter', font=('Segoe UI', 9), command=self._refresh_details_grid).pack(side='left')
        # Details grid
        self.details_tree = ttk.Treeview(details_frame, columns=[], show='headings', height=10, style='Custom.Treeview')
        self.details_tree.pack(fill='both', expand=True)
        # Pagination controls for details grid
        pag_frame = tk.Frame(details_frame, bg=self.app_colors['bg'])
        pag_frame.pack(fill='x', pady=(4, 0))
        self.details_page_var = tk.IntVar(value=1)
        self.details_page_size = 25
        self.details_max_page = 1
        tk.Button(pag_frame, text='Prev', command=self._details_prev_page).pack(side='left', padx=4)
        self.details_page_label = tk.Label(pag_frame, text='Page 1/1', font=('Segoe UI', 10), bg=self.app_colors['bg'], fg=self.app_colors['fg'])
        self.details_page_label.pack(side='left', padx=8)
        tk.Button(pag_frame, text='Next', command=self._details_next_page).pack(side='left', padx=4)
        # Track selected table for details
        self.details_table_name = None
        self.details_columns = []
        self._refresh_summary_grid()
        self._refresh_details_grid(force_clear=True)

    def _on_report_db_change(self):
        self._refresh_summary_grid()
        self._refresh_details_grid(force_clear=True)

    def _on_summary_row_select(self, event):
        selected = self.summary_tree.selection()
        if not selected:
            self.details_table_name = None
            self._refresh_details_grid(force_clear=True)
            return
        item = self.summary_tree.item(selected[0])
        values = item['values']
        if not values:
            self.details_table_name = None
            self._refresh_details_grid(force_clear=True)
            return
        # Assume table_name is always the second column
        self.details_table_name = values[1]
        self.details_page_var.set(1)
        # Auto-set status filter to match the selected row's status
        status_val = values[-1]
        if status_val == '✅':
            self.summary_status_var.set('Matched')
        elif status_val == '❌':
            self.summary_status_var.set('Mismatched')
        # else do not change filter for custom/other statuses
        self._refresh_details_grid()

    def _fetch_summary_data(self):
        """Fetch summary data from the selected PostgreSQL database (datareconciliator_summary table)."""
        db_name = self.report_db_var.get().strip() if hasattr(self, 'report_db_var') else self.config.postgres.get('db', '')
        if not db_name:
            return []
        self.config.postgres['db'] = db_name
        self.config.save()
        pg_conn_str = self.config.build_pg_conn_str()
        try:
            engine = sqlalchemy.create_engine(pg_conn_str)
            with engine.connect() as conn:
                result = conn.execute(sqlalchemy.text("""
                    SELECT * FROM public.datareconciliator_summary
                    ORDER BY validation_timestamp DESC
                """))
                rows = [dict(row._mapping) for row in result]
            return rows
        except Exception as e:
            self.log(f"[ERROR] Could not fetch summary data: {e}", success=False, level='ERROR')
            return []

    def _fetch_details_data(self, table_name):
        """Fetch details data for a table from the selected PostgreSQL database (datareconciliator_details table)."""
        db_name = self.report_db_var.get().strip() if hasattr(self, 'report_db_var') else self.config.postgres.get('db', '')
        if not db_name:
            return [], []
        self.config.postgres['db'] = db_name
        self.config.save()
        pg_conn_str = self.config.build_pg_conn_str()
        try:
            engine = sqlalchemy.create_engine(pg_conn_str)
            with engine.connect() as conn:
                # Get all columns in details table
                col_result = conn.execute(sqlalchemy.text("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'datareconciliator_details'
                    ORDER BY ordinal_position
                """))
                columns = [row[0] for row in col_result]
                # Determine correct column for filtering
                if 'table_name' in columns:
                    filter_col = 'table_name'
                elif 'table' in columns:
                    filter_col = '"table"'  # quoted for reserved word
                else:
                    filter_col = columns[0]  # fallback, should not happen
                # Build query string
                query = f"SELECT * FROM public.datareconciliator_details WHERE {filter_col} = :table_name"
                result = conn.execute(sqlalchemy.text(query), {'table_name': table_name})
                rows = [dict(row._mapping) for row in result]
            return rows, columns
        except Exception as e:
            self.log(f"[ERROR] Could not fetch details data: {e}", success=False, level='ERROR')
            return [], []

    def _refresh_summary_grid(self, sort_by_table=False):
        for iid in self.summary_tree.get_children():
            self.summary_tree.delete(iid)
        summary_data = self._fetch_summary_data()
        if sort_by_table:
            summary_data = sorted(summary_data, key=lambda x: x.get('table_name', ''))
        table_filter = self.summary_search_var.get().strip().lower()
        status_filter = self.summary_status_var.get()
        filtered = []
        for row in summary_data:
            # Table name filter (always applies)
            if table_filter and table_filter not in str(row.get('table_name', '')).lower():
                continue
            # Status filter logic
            if status_filter == 'All':
                filtered.append(row)  # Show all records regardless of status
            elif status_filter == 'Matched':
                # Accept both 'Matched' and '✅' as status
                if str(row.get('status', '')).strip().lower() in ('matched', '✅'):
                    filtered.append(row)
            elif status_filter == 'Mismatched':
                # Accept both 'Mismatched' and '❌' as status
                if str(row.get('status', '')).strip().lower() in ('mismatched', '❌'):
                    filtered.append(row)
        # Pagination
        page = self.summary_page_var.get() if hasattr(self, 'summary_page_var') else 1
        page_size = getattr(self, 'summary_page_size', 25)
        total = len(filtered)
        max_page = max(1, (total + page_size - 1) // page_size)
        self.summary_max_page = max_page
        start = (page - 1) * page_size
        end = start + page_size
        page_rows = filtered[start:end]
        for row in page_rows:
            # Show status as icon for clarity
            status_val = str(row.get('status', ''))
            if status_val.strip().lower() in ('matched', '✅'):
                status_display = '✅'
            elif status_val.strip().lower() in ('mismatched', '❌'):
                status_display = '❌'
            else:
                status_display = status_val
            values = [row.get(col, '') if col != 'status' else status_display for col in self.summary_columns]
            self.summary_tree.insert('', 'end', values=values)
        self.summary_total_var.set(f'Total: {total}')
        if hasattr(self, 'summary_page_label'):
            self.summary_page_label.config(text=f'Page {page}/{max_page}')

    def _summary_prev_page(self):
        """Go to previous page in summary grid."""
        page = self.summary_page_var.get()
        if page > 1:
            self.summary_page_var.set(page - 1)
            self._refresh_summary_grid()

    def _summary_next_page(self):
        """Go to next page in summary grid."""
        page = self.summary_page_var.get()
        max_page = getattr(self, 'summary_max_page', 1)
        if page < max_page:
            self.summary_page_var.set(page + 1)
            self._refresh_summary_grid()

    def _details_prev_page(self):
        """Go to previous page in details grid."""
        page = self.details_page_var.get()
        if page > 1:
            self.details_page_var.set(page - 1)
            self._refresh_details_grid()

    def _details_next_page(self):
        """Go to next page in details grid."""
        page = self.details_page_var.get()
        max_page = getattr(self, 'details_max_page', 1)
        if page < max_page:
            self.details_page_var.set(page + 1)
            self._refresh_details_grid()

    def _refresh_details_grid(self, force_clear=False):
        for iid in self.details_tree.get_children():
            self.details_tree.delete(iid)
        if force_clear or not self.details_table_name:
            self.details_total_var.set('Total: 0')
            self.details_page_label.config(text='Page 1/1')
            return
        details_data, columns = self._fetch_details_data(self.details_table_name)
        # Dynamically set columns if changed
        if hasattr(self, 'details_columns'):
            if columns != self.details_columns:
                self.details_tree['columns'] = columns
                for col in self.details_tree['columns']:
                    self.details_tree.heading(col, text=col)
                    self.details_tree.column(col, width=120, anchor='w')
                self.details_columns = columns
        else:
            self.details_tree['columns'] = columns
            for col in columns:
                self.details_tree.heading(col, text=col)
                self.details_tree.column(col, width=120, anchor='w')
            self.details_columns = columns
        filter_val = self.details_filter_var.get().strip().lower()
        filtered = [row for row in details_data if not filter_val or filter_val in str(row).lower()]
        page = self.details_page_var.get()
        page_size = self.details_page_size
        total = len(filtered)
        max_page = max(1, (total + page_size - 1) // page_size)
        self.details_max_page = max_page
        start = (page - 1) * page_size
        end = start + page_size
        page_rows = filtered[start:end]
        for row in page_rows:
            values = [row.get(col, '') for col in self.details_columns]
            self.details_tree.insert('', 'end', values=values)
        self.details_total_var.set(f'Total: {total}')
        self.details_page_label.config(text=f'Page {page}/{max_page}')

    def _show_help(self):
        help_text = (
            "RCW Data Reconciliator Help:\n\n"
            "Scalable SQL Server ↔ PostgreSQL Data Validation Tool\n\n"
            "Features:\n"
            "- Configure SQL Server and PostgreSQL connections.\n"
            "- Select tables for validation with easy checkboxes.\n"
            "- Start validation and monitor progress with a live progress bar.\n"
            "- View summary and detailed reports with filtering and pagination.\n"
            "- Export validation results to CSV.\n"
            "- Minimal log viewer for table-wise events.\n"
            "- Global notifications for important events.\n"
            "- Settings page to choose output directory.\n"
            "- Modern, responsive UI with sidebar navigation.\n\n"
            "How to Use:\n"
            "1. Configure SQL Server and PostgreSQL connection details in Home.\n"
            "2. Select tables to validate and click 'Update Table'.\n"
                       "3. Click 'Start Validation' to begin.\n"
            "4. Monitor progress and view logs.\n"
            "5. Switch to the Report page for summary and details.\n"
            "6. Use Settings to change output directory.\n\n"
            "Support:\n"
            "For advanced help, contact your administrator or support team."
        )
        messagebox.showinfo(
            "Help",
            help_text
        )

def main():
    app = DataReconciliatorApp()
    app.mainloop()

if __name__ == "__main__":
    main()
