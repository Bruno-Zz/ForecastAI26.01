/**
 * AdminPanel — SuperAdmin only
 *
 * Displays all tenant accounts and allows creating, cloning, and
 * deactivating accounts.  Also lists superAdmin users and allows
 * adding new ones.
 *
 * Routes:  /api/admin/accounts
 *          /api/admin/superadmins
 */

import React, { useState, useEffect, useCallback } from 'react';
import api from '../utils/api';

// ─── helpers ────────────────────────────────────────────────────────────────

function slugify(str) {
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 63);
}

function Badge({ children, color = 'gray' }) {
  const colours = {
    green:  'bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300',
    red:    'bg-red-100 text-red-700 dark:bg-red-900/40 dark:text-red-300',
    amber:  'bg-amber-100 text-amber-700 dark:bg-amber-900/40 dark:text-amber-300',
    blue:   'bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300',
    gray:   'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-300',
    purple: 'bg-purple-100 text-purple-700 dark:bg-purple-900/40 dark:text-purple-300',
  };
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${colours[color] || colours.gray}`}>
      {children}
    </span>
  );
}

function Modal({ title, onClose, children }) {
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl w-full max-w-lg">
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-lg font-semibold text-gray-900 dark:text-white">{title}</h2>
          <button onClick={onClose} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200">
            <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
            </svg>
          </button>
        </div>
        <div className="px-6 py-4">{children}</div>
      </div>
    </div>
  );
}

function Input({ label, id, ...props }) {
  return (
    <div>
      {label && (
        <label htmlFor={id} className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          {label}
        </label>
      )}
      <input
        id={id}
        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white placeholder-gray-400 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none text-sm"
        {...props}
      />
    </div>
  );
}

function Select({ label, id, children, ...props }) {
  return (
    <div>
      {label && (
        <label htmlFor={id} className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">
          {label}
        </label>
      )}
      <select
        id={id}
        className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-700 text-gray-900 dark:text-white focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none text-sm"
        {...props}
      >
        {children}
      </select>
    </div>
  );
}

// ─── polling hook ────────────────────────────────────────────────────────────

function useProvisionPoll(jobId, onComplete) {
  useEffect(() => {
    if (!jobId) return;
    const id = setInterval(async () => {
      try {
        const res = await api.get(`/admin/accounts/${jobId}/provision-status`);
        const job = res.data;
        if (job.status === 'complete' || job.status === 'failed') {
          clearInterval(id);
          onComplete(job);
        }
      } catch {
        clearInterval(id);
      }
    }, 2000);
    return () => clearInterval(id);
  }, [jobId, onComplete]);
}

// ════════════════════════════════════════════════════════════════════════════
// Main component
// ════════════════════════════════════════════════════════════════════════════

export default function AdminPanel() {
  const [accounts, setAccounts] = useState([]);
  const [superAdmins, setSuperAdmins] = useState([]);
  const [loadingAccounts, setLoadingAccounts] = useState(true);
  const [error, setError] = useState('');
  const [toast, setToast] = useState('');

  // Modals
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showDeleteModal, setShowDeleteModal] = useState(null); // account obj
  const [showSAModal, setShowSAModal] = useState(false);

  // Create form
  const [createMode, setCreateMode] = useState('empty'); // 'empty' | 'clone'
  const [createName, setCreateName] = useState('');
  const [createDbName, setCreateDbName] = useState('');
  const [createSchema, setCreateSchema] = useState('zcube');
  const [cloneSourceId, setCloneSourceId] = useState('');
  const [creating, setCreating] = useState(false);
  const [activeJobId, setActiveJobId] = useState(null);
  const [jobStatus, setJobStatus] = useState('');

  // Delete confirm
  const [deleteConfirmText, setDeleteConfirmText] = useState('');
  const [deleting, setDeleting] = useState(false);

  // SuperAdmin form
  const [saEmail, setSaEmail] = useState('');
  const [saName, setSaName] = useState('');
  const [saPassword, setSaPassword] = useState('');
  const [saCreating, setSaCreating] = useState(false);

  const showToast = (msg) => {
    setToast(msg);
    setTimeout(() => setToast(''), 4000);
  };

  // ── Fetch accounts & superAdmins ──────────────────────────────────────
  const fetchAll = useCallback(async () => {
    setLoadingAccounts(true);
    setError('');
    try {
      const [accRes, saRes] = await Promise.all([
        api.get('/admin/accounts'),
        api.get('/admin/superadmins'),
      ]);
      setAccounts(accRes.data.accounts || []);
      setSuperAdmins(saRes.data.superadmins || []);
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to load data');
    } finally {
      setLoadingAccounts(false);
    }
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  // ── Provision job polling ─────────────────────────────────────────────
  useProvisionPoll(activeJobId, (job) => {
    if (job.status === 'complete') {
      showToast(`Account "${job.display_name}" provisioned successfully`);
      fetchAll();
    } else {
      setError(`Provisioning failed: ${job.error || 'unknown error'}`);
    }
    setActiveJobId(null);
    setJobStatus('');
    setCreating(false);
  });

  // ── Create account ────────────────────────────────────────────────────
  const handleCreate = async () => {
    if (!createName.trim()) { setError('Display name is required'); return; }
    if (!createDbName.trim()) { setError('DB name is required'); return; }
    if (createMode === 'clone' && !cloneSourceId) { setError('Select a source account to clone'); return; }
    setCreating(true);
    setError('');
    try {
      const body = {
        display_name: createName.trim(),
        db_name: createDbName.trim(),
        schema_name: createSchema.trim() || 'zcube',
      };
      if (createMode === 'clone') {
        body.clone_from_account_id = cloneSourceId;
      }
      const res = await api.post('/admin/accounts', body);
      setActiveJobId(res.data.job_id);
      setJobStatus('queued');
      setShowCreateModal(false);
      // Reset form
      setCreateName(''); setCreateDbName(''); setCreateSchema('zcube');
      setCreateMode('empty'); setCloneSourceId('');
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to create account');
      setCreating(false);
    }
  };

  // ── Deactivate account ────────────────────────────────────────────────
  const handleDelete = async () => {
    if (!showDeleteModal) return;
    if (deleteConfirmText !== showDeleteModal.display_name) {
      setError('Account name does not match');
      return;
    }
    setDeleting(true);
    setError('');
    try {
      await api.delete(`/admin/accounts/${showDeleteModal.id}`);
      showToast(`Account "${showDeleteModal.display_name}" deactivated`);
      setShowDeleteModal(null);
      setDeleteConfirmText('');
      fetchAll();
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to deactivate account');
    } finally {
      setDeleting(false);
    }
  };

  // ── Create superAdmin ─────────────────────────────────────────────────
  const handleCreateSA = async () => {
    if (!saEmail.trim() || !saPassword) { setError('Email and password are required'); return; }
    setSaCreating(true);
    setError('');
    try {
      await api.post('/admin/superadmins', {
        email: saEmail.trim(),
        display_name: saName.trim() || saEmail.split('@')[0],
        password: saPassword,
      });
      showToast('SuperAdmin created');
      setShowSAModal(false);
      setSaEmail(''); setSaName(''); setSaPassword('');
      fetchAll();
    } catch (e) {
      setError(e.response?.data?.detail || 'Failed to create superAdmin');
    } finally {
      setSaCreating(false);
    }
  };

  // ─── Render ───────────────────────────────────────────────────────────
  return (
    <div className="p-6 max-w-5xl mx-auto">
      {/* Header */}
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-white flex items-center gap-2">
          🏢 Account Admin
        </h1>
        <p className="text-sm text-gray-500 dark:text-gray-400 mt-1">
          Manage tenant accounts and superAdmin users. SuperAdmin only.
        </p>
      </div>

      {/* Error */}
      {error && (
        <div className="mb-4 p-3 bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg text-red-700 dark:text-red-300 text-sm flex justify-between">
          {error}
          <button onClick={() => setError('')} className="ml-2 font-bold">×</button>
        </div>
      )}

      {/* Toast */}
      {toast && (
        <div className="mb-4 p-3 bg-emerald-50 dark:bg-emerald-900/30 border border-emerald-200 dark:border-emerald-700 rounded-lg text-emerald-700 dark:text-emerald-300 text-sm">
          {toast}
        </div>
      )}

      {/* Job status banner */}
      {activeJobId && (
        <div className="mb-4 p-3 bg-blue-50 dark:bg-blue-900/30 border border-blue-200 dark:border-blue-700 rounded-lg text-blue-700 dark:text-blue-300 text-sm flex items-center gap-2">
          <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-600 flex-shrink-0" />
          Provisioning account — status: <strong>{jobStatus}</strong> — please wait…
        </div>
      )}

      {/* ── Accounts section ──────────────────────────────────────────── */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700 mb-6">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">Tenant Accounts</h2>
          <button
            onClick={() => setShowCreateModal(true)}
            disabled={!!activeJobId}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors disabled:opacity-50"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            New Account
          </button>
        </div>

        {loadingAccounts ? (
          <div className="flex justify-center py-8">
            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-blue-600" />
          </div>
        ) : accounts.length === 0 ? (
          <p className="text-center text-gray-400 dark:text-gray-500 py-8 text-sm">No accounts found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 dark:border-gray-700">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Display Name</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Database</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Schema</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Created</th>
                  <th className="px-5 py-3" />
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
                {accounts.map((acc) => (
                  <tr key={acc.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/30 transition-colors">
                    <td className="px-5 py-3 font-medium text-gray-900 dark:text-white">{acc.display_name}</td>
                    <td className="px-5 py-3 font-mono text-xs text-gray-500 dark:text-gray-400">{acc.db_name}</td>
                    <td className="px-5 py-3 font-mono text-xs text-gray-500 dark:text-gray-400">{acc.schema_name}</td>
                    <td className="px-5 py-3">
                      {acc.is_active
                        ? <Badge color="green">Active</Badge>
                        : <Badge color="red">Inactive</Badge>}
                    </td>
                    <td className="px-5 py-3 text-xs text-gray-400 dark:text-gray-500">
                      {acc.created_at ? new Date(acc.created_at).toLocaleDateString() : '—'}
                    </td>
                    <td className="px-5 py-3">
                      <div className="flex items-center gap-2 justify-end">
                        <button
                          onClick={() => {
                            setCreateMode('clone');
                            setCloneSourceId(acc.id);
                            setShowCreateModal(true);
                          }}
                          disabled={!acc.is_active || !!activeJobId}
                          className="text-xs text-blue-600 dark:text-blue-400 hover:underline disabled:opacity-40 disabled:cursor-not-allowed"
                          title="Clone this account"
                        >
                          Clone
                        </button>
                        {acc.is_active && (
                          <button
                            onClick={() => setShowDeleteModal(acc)}
                            className="text-xs text-red-500 dark:text-red-400 hover:underline"
                            title="Deactivate this account"
                          >
                            Deactivate
                          </button>
                        )}
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── SuperAdmins section ───────────────────────────────────────── */}
      <div className="bg-white dark:bg-gray-800 rounded-xl border border-gray-200 dark:border-gray-700">
        <div className="flex items-center justify-between px-5 py-4 border-b border-gray-200 dark:border-gray-700">
          <h2 className="text-base font-semibold text-gray-900 dark:text-white">SuperAdmin Users</h2>
          <button
            onClick={() => setShowSAModal(true)}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-amber-600 hover:bg-amber-700 text-white text-sm font-medium rounded-lg transition-colors"
          >
            <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
            </svg>
            Add SuperAdmin
          </button>
        </div>
        {superAdmins.length === 0 ? (
          <p className="text-center text-gray-400 dark:text-gray-500 py-8 text-sm">No superAdmins found</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-100 dark:border-gray-700">
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Email</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Display Name</th>
                  <th className="text-left px-5 py-3 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Auth</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-50 dark:divide-gray-700/50">
                {superAdmins.map((sa) => (
                  <tr key={sa.id} className="hover:bg-gray-50 dark:hover:bg-gray-700/30">
                    <td className="px-5 py-3 text-gray-900 dark:text-white">{sa.email}</td>
                    <td className="px-5 py-3 text-gray-600 dark:text-gray-300">{sa.display_name}</td>
                    <td className="px-5 py-3">
                      <Badge color={sa.auth_provider === 'local' ? 'blue' : 'purple'}>
                        {sa.auth_provider}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* ── Create account modal ──────────────────────────────────────── */}
      {showCreateModal && (
        <Modal
          title={createMode === 'clone' ? 'Clone Account' : 'New Account'}
          onClose={() => { setShowCreateModal(false); setCreateMode('empty'); setCloneSourceId(''); setCreateName(''); setCreateDbName(''); setCreateSchema('zcube'); setError(''); }}
        >
          <div className="space-y-4">
            {/* Mode toggle */}
            <div className="flex gap-2">
              <button
                onClick={() => setCreateMode('empty')}
                className={`flex-1 py-2 text-sm font-medium rounded-lg transition-colors ${createMode === 'empty' ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
              >
                Empty Account
              </button>
              <button
                onClick={() => setCreateMode('clone')}
                className={`flex-1 py-2 text-sm font-medium rounded-lg transition-colors ${createMode === 'clone' ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
              >
                Clone Existing
              </button>
            </div>

            {createMode === 'clone' && (
              <Select
                label="Clone source"
                id="clone-source"
                value={cloneSourceId}
                onChange={(e) => setCloneSourceId(e.target.value)}
              >
                <option value="">— select account —</option>
                {accounts.filter(a => a.is_active).map(a => (
                  <option key={a.id} value={a.id}>{a.display_name} ({a.db_name})</option>
                ))}
              </Select>
            )}

            <Input
              label="Display Name"
              id="create-name"
              placeholder="Acme Corp"
              value={createName}
              onChange={(e) => {
                setCreateName(e.target.value);
                if (!createDbName || createDbName === slugify(createName)) {
                  setCreateDbName(slugify(e.target.value));
                }
              }}
            />
            <Input
              label="Database Name"
              id="create-db"
              placeholder="forecastai_acme"
              value={createDbName}
              onChange={(e) => setCreateDbName(e.target.value)}
            />
            <Input
              label="Schema Name"
              id="create-schema"
              placeholder="zcube"
              value={createSchema}
              onChange={(e) => setCreateSchema(e.target.value)}
            />

            {error && (
              <p className="text-red-600 dark:text-red-400 text-sm">{error}</p>
            )}

            <div className="flex gap-2 pt-2">
              <button
                onClick={() => { setShowCreateModal(false); setError(''); }}
                className="flex-1 py-2 text-sm border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleCreate}
                disabled={creating}
                className="flex-1 py-2 text-sm bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                {creating ? 'Starting…' : createMode === 'clone' ? 'Clone Account' : 'Create Account'}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* ── Delete confirm modal ──────────────────────────────────────── */}
      {showDeleteModal && (
        <Modal
          title="Deactivate Account"
          onClose={() => { setShowDeleteModal(null); setDeleteConfirmText(''); setError(''); }}
        >
          <div className="space-y-4">
            <p className="text-sm text-gray-600 dark:text-gray-300">
              This will set the account to <strong>inactive</strong>. The underlying database will
              NOT be dropped — use psql directly if you want to permanently remove it.
            </p>
            <p className="text-sm text-gray-700 dark:text-gray-200">
              To confirm, type the account display name: <strong>{showDeleteModal.display_name}</strong>
            </p>
            <Input
              id="delete-confirm"
              placeholder={showDeleteModal.display_name}
              value={deleteConfirmText}
              onChange={(e) => setDeleteConfirmText(e.target.value)}
            />
            {error && <p className="text-red-600 dark:text-red-400 text-sm">{error}</p>}
            <div className="flex gap-2 pt-2">
              <button
                onClick={() => { setShowDeleteModal(null); setDeleteConfirmText(''); setError(''); }}
                className="flex-1 py-2 text-sm border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleDelete}
                disabled={deleting || deleteConfirmText !== showDeleteModal.display_name}
                className="flex-1 py-2 text-sm bg-red-600 hover:bg-red-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                {deleting ? 'Deactivating…' : 'Deactivate'}
              </button>
            </div>
          </div>
        </Modal>
      )}

      {/* ── Add superAdmin modal ──────────────────────────────────────── */}
      {showSAModal && (
        <Modal title="Add SuperAdmin" onClose={() => { setShowSAModal(false); setSaEmail(''); setSaName(''); setSaPassword(''); setError(''); }}>
          <div className="space-y-4">
            <Input
              label="Email"
              id="sa-email"
              type="email"
              placeholder="admin@company.com"
              value={saEmail}
              onChange={(e) => setSaEmail(e.target.value)}
            />
            <Input
              label="Display Name"
              id="sa-name"
              placeholder="Jane Smith"
              value={saName}
              onChange={(e) => setSaName(e.target.value)}
            />
            <Input
              label="Password"
              id="sa-password"
              type="password"
              placeholder="Strong password"
              value={saPassword}
              onChange={(e) => setSaPassword(e.target.value)}
            />
            {error && <p className="text-red-600 dark:text-red-400 text-sm">{error}</p>}
            <div className="flex gap-2 pt-2">
              <button
                onClick={() => { setShowSAModal(false); setError(''); }}
                className="flex-1 py-2 text-sm border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleCreateSA}
                disabled={saCreating}
                className="flex-1 py-2 text-sm bg-amber-600 hover:bg-amber-700 text-white font-medium rounded-lg transition-colors disabled:opacity-50"
              >
                {saCreating ? 'Creating…' : 'Create SuperAdmin'}
              </button>
            </div>
          </div>
        </Modal>
      )}
    </div>
  );
}
