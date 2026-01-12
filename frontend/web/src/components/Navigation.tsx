import { Link } from 'react-router-dom';
import { useAuth } from '../../../shared/hooks/useAuth';

export function Navigation() {
  const { user, logout, isAuthenticated } = useAuth();

  const handleLogout = async () => {
    try {
      await logout();
      window.location.href = '/';
    } catch (error) {
      console.error('Logout failed:', error);
    }
  };

  return (
    <nav className="bg-dark-green-950/90 backdrop-blur-md text-white shadow-lg border-b border-green-700/30 font-display">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo/Brand */}
          <Link to="/" className="flex items-center hover:opacity-80 transition">
            <img 
              src="/Game_and_logz_logo_prototype.png" 
              alt="Game and Logz Logo" 
              className="h-12"
            />
          </Link>

          {/* Navigation Links */}
          <div className="flex items-center space-x-6">
            <Link 
              to="/" 
              className="hover:text-green-400 transition font-medium"
            >
              Home
            </Link>
            <Link 
              to="/search" 
              className="hover:text-green-400 transition font-medium"
            >
              Search
            </Link>
            
            {isAuthenticated ? (
              <>
                <Link 
                  to="/backlog" 
                  className="hover:text-green-400 transition font-medium"
                >
                  {user?.display_name || user?.email?.split('@')[0] || 'My Backlog'}
                </Link>
                <button
                  onClick={handleLogout}
                  className="hover:text-red-400 transition font-medium"
                >
                  Logout
                </button>
              </>
            ) : (
              <div className="flex items-center space-x-4">
                <Link 
                  to="/login" 
                  className="hover:text-green-400 transition font-medium"
                >
                  Login
                </Link>
                <Link 
                  to="/register" 
                  className="bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 px-4 py-2 rounded-md transition font-medium shadow-lg"
                >
                  Register
                </Link>
              </div>
            )}
          </div>
        </div>
      </div>
    </nav>
  );
}

