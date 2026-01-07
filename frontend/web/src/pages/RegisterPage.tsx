import { useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useAuth } from '../../../shared/hooks/useAuth';

export function RegisterPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const { register, isLoading, error } = useAuth();
  const navigate = useNavigate();

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    
    try {
      await register(email, password, displayName);
      window.location.href = '/backlog';
    } catch (err) {
      // Error is handled by useAuth hook
    }
  };

  return (
    <div className="min-h-screen flex items-center justify-center px-4">
      <div className="max-w-md w-full bg-dark-green-900/50 backdrop-blur-sm rounded-lg p-8 border border-green-700/30">
        <h2 className="text-3xl font-bold text-green-300 mb-6 text-center">Register</h2>
        
        {error && (
          <div className="bg-red-900/50 border border-red-700 text-white px-4 py-3 rounded-lg mb-6">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} className="space-y-6">
          <div>
            <label htmlFor="displayName" className="block text-sm font-medium text-green-200 mb-2">
              Display Name
            </label>
            <input
              id="displayName"
              type="text"
              value={displayName}
              onChange={(e) => setDisplayName(e.target.value)}
              required
              minLength={3}
              className="w-full px-4 py-3 rounded-lg bg-dark-green-800 text-white border border-green-600 focus:outline-none focus:border-green-500 placeholder-green-300/50"
              placeholder="CoolGamer123"
            />
          </div>

          <div>
            <label htmlFor="email" className="block text-sm font-medium text-green-200 mb-2">
              Email
            </label>
            <input
              id="email"
              type="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
              className="w-full px-4 py-3 rounded-lg bg-dark-green-800 text-white border border-green-600 focus:outline-none focus:border-green-500 placeholder-green-300/50"
              placeholder="your@email.com"
            />
          </div>

          <div>
            <label htmlFor="password" className="block text-sm font-medium text-green-200 mb-2">
              Password
            </label>
            <input
              id="password"
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
              minLength={6}
              className="w-full px-4 py-3 rounded-lg bg-dark-green-800 text-white border border-green-600 focus:outline-none focus:border-green-500 placeholder-green-300/50"
              placeholder="••••••••"
            />
            <p className="text-sm text-green-200 mt-1">Minimum 6 characters</p>
          </div>

          <button
            type="submit"
            disabled={isLoading}
            className="w-full bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-700 hover:to-emerald-700 disabled:from-dark-green-800 disabled:to-dark-green-800 text-white py-3 rounded-lg font-semibold transition shadow-lg"
          >
            {isLoading ? 'Creating account...' : 'Register'}
          </button>
        </form>

        <p className="mt-6 text-center text-green-200">
          Already have an account?{' '}
          <Link to="/login" className="text-green-400 hover:text-green-300">
            Login here
          </Link>
        </p>
      </div>
    </div>
  );
}

