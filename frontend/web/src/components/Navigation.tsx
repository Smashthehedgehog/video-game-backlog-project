import { Link } from 'react-router-dom';
import { useAuth } from '../../../shared/hooks/useAuth';

export function Navigation() {
  const { user, logout, isAuthenticated } = useAuth();

  const handleLogout = async () => {
    try {
      await logout();
    } catch (error) {
      console.error('Logout failed:', error);
    }
  };

  return (
    <nav className="bg-gray-900 text-white shadow-lg">
      <div className="container mx-auto px-4">
        <div className="flex items-center justify-between h-16">
          {/* Logo/Brand */}
          <Link to="/" className="text-2xl font-bold hover:text-blue-400 transition">
            VG Backlog
          </Link>

          {/* Navigation Links */}
          <div className="flex items-center space-x-6">
            <Link 
              to="/" 
              className="hover:text-blue-400 transition font-medium"
            >
              Home
            </Link>
            <Link 
              to="/search" 
              className="hover:text-blue-400 transition font-medium"
            >
              Search
            </Link>
            
            {isAuthenticated ? (
              <>
                <Link 
                  to="/backlog" 
                  className="hover:text-blue-400 transition font-medium"
                >
                  My Backlog
                </Link>
                <div className="flex items-center space-x-4">
                  <span className="text-gray-300 text-sm">
                    {user?.email}
                  </span>
                  <button
                    onClick={handleLogout}
                    className="bg-red-600 hover:bg-red-700 px-4 py-2 rounded-md transition font-medium"
                  >
                    Logout
                  </button>
                </div>
              </>
            ) : (
              <div className="flex items-center space-x-4">
                <Link 
                  to="/login" 
                  className="hover:text-blue-400 transition font-medium"
                >
                  Login
                </Link>
                <Link 
                  to="/register" 
                  className="bg-blue-600 hover:bg-blue-700 px-4 py-2 rounded-md transition font-medium"
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

