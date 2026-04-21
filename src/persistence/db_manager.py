from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from rich.console import Console
from src.config import DATABASE_URL
from src.models.database import Base

console = Console()

class DatabaseManager:
    """
    Manages database connections, session creation, and table setup.
    
    Groups database logic into a single controller to handle the 
    engine lifecycle and session generation.
    """
    
    def __init__(self, database_url: str = DATABASE_URL):
        self.engine = create_engine(database_url, echo=False)
        
        self.SessionFactory = sessionmaker(
            bind=self.engine,
            expire_on_commit=False,
        )
    
    def create_tables(self):
        Base.metadata.create_all(self.engine)
        console.print("[green]✅ Database tables created/verified[/green]")
    
    def get_session(self) -> Session:
        return self.SessionFactory()
    
    def get_session_context(self):
        return _SessionContext(self.SessionFactory)
    
    def drop_all_tables(self):
        Base.metadata.drop_all(self.engine)
        console.print("[yellow]⚠️  All database tables dropped[/yellow]")


class _SessionContext:
    """
    Context manager for safe database transactions.
    
    Automates the 'Unit of Work' pattern:
    1. Opens session on __enter__
    2. Commits if successful, or rolls back if an exception occurs
    3. Always closes the session on __exit__
    """
    
    def __init__(self, session_factory):
        self.session_factory = session_factory
        self.session = None
    
    def __enter__(self) -> Session:
        self.session = self.session_factory()
        return self.session
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.session.rollback()
        else:
            self.session.commit()
        self.session.close()