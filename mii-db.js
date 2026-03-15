/**
 * MiiDB — IndexedDB wrapper for MII Supervisor Hub
 *
 * Replaces raw localStorage with IndexedDB for reliable offline persistence.
 * Dual-writes to localStorage during migration period.
 * Queues every mutation to sync_queue for background server sync.
 *
 * Usage:
 *   await MiiDB.save('defects', record);
 *   const all = await MiiDB.getAll('defects');
 *   const one = await MiiDB.get('defects', 'DEF-432567');
 *   await MiiDB.remove('defects', 'DEF-432567');
 *   await MiiDB.saveChecklist(stateObj);
 *   const state = await MiiDB.getChecklist();
 */
(function (global) {
  'use strict';

  const DB_NAME = 'mii-hub';
  const DB_VERSION = 12;

  // IndexedDB store name → localStorage key mapping
  const STORE_LS_MAP = {
    inspections: 'mii_inspections',
    defects: 'mii_defects',
    requisitions: 'mii_requisitions',
    bookings: 'mii_bookings',
    goods_inward: 'mii_goods_inward',
    workplace_inspections: 'mii_workplace_inspections',
    near_miss: 'mii_near_miss',
    hot_work: 'mii_hot_work',
    daily_reports: 'mii_daily_reports',
    tools: 'mii_tools',
    havs_entries: 'mii_havs_entries',
    sheq_observations: 'mii_sheq_observations',
    powra: 'mii_powra',
    coshh_assessments: 'mii_coshh_assessments',
    msds_substances: 'mii_msds_substances',
    checklist_state: 'mii_supervisor_checklist_v4',
  };

  // Stores that hold arrays of records (keyed by `id`)
  const ARRAY_STORES = [
    'inspections',
    'defects',
    'requisitions',
    'bookings',
    'goods_inward',
    'workplace_inspections',
    'near_miss',
    'hot_work',
    'daily_reports',
    'tools',
    'havs_entries',
    'sheq_observations',
    'powra',
    'certificates',
    'rams_documents',
    'cost_jobs',
    'cost_categories',
    'cost_transactions',
    'cost_supplier_mappings',
    'cost_labour_mappings',
    'cost_imports',
    'cost_rate_cards',
    'cost_rate_grades',
    'cost_estimates',
    'cost_estimate_items',
    'coshh_assessments',
    'msds_substances',
  ];

  // Reference data stores (pulled from server, not pushed via sync_queue)
  const REF_STORES = ['employees'];

  // All stores including non-array ones and internal queue
  const ALL_STORES = [...ARRAY_STORES, ...REF_STORES, 'checklist_state', 'sync_queue'];

  let _db = null;
  let _dbReady = null; // Promise that resolves when DB is open + migrated

  // ─── Database lifecycle ────────────────────────────────────────

  function openDB() {
    if (_dbReady) return _dbReady;

    _dbReady = new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, DB_VERSION);

      req.onupgradeneeded = function (e) {
        const db = e.target.result;

        // Create array-based stores with keyPath=id
        // Stores with custom indexes are handled individually below
        const INDEXED_STORES = [
          'daily_reports', 'havs_entries', 'certificates', 'rams_documents',
          'cost_jobs', 'cost_categories', 'cost_transactions',
          'cost_supplier_mappings', 'cost_labour_mappings', 'cost_imports',
          'cost_rate_cards', 'cost_rate_grades',
          'cost_estimates', 'cost_estimate_items',
        ];
        ARRAY_STORES.forEach((name) => {
          if (!INDEXED_STORES.includes(name) && !db.objectStoreNames.contains(name)) {
            db.createObjectStore(name, { keyPath: 'id' });
          }
        });

        // daily_reports store with indexes for lookup
        if (!db.objectStoreNames.contains('daily_reports')) {
          const drStore = db.createObjectStore('daily_reports', { keyPath: 'id' });
          drStore.createIndex('report_date', 'reportDate', { unique: false });
          drStore.createIndex('supervisor_name', 'supervisorName', { unique: false });
          drStore.createIndex('client_id', 'clientId', { unique: true });
        }

        // havs_entries indexes for date and worker lookups
        if (!db.objectStoreNames.contains('havs_entries')) {
          const havsStore = db.createObjectStore('havs_entries', { keyPath: 'id' });
          havsStore.createIndex('date', 'date', { unique: false });
          havsStore.createIndex('workerId', 'workerId', { unique: false });
        }

        // certificates store with indexes for employee and training type lookups
        if (!db.objectStoreNames.contains('certificates')) {
          const certStore = db.createObjectStore('certificates', { keyPath: 'id' });
          certStore.createIndex('employee_number', 'employee_number', { unique: false });
          certStore.createIndex('training_type', 'training_type', { unique: false });
        }

        // rams_documents store with indexes for status and RAMS number lookups
        if (!db.objectStoreNames.contains('rams_documents')) {
          const ramsStore = db.createObjectStore('rams_documents', { keyPath: 'id' });
          ramsStore.createIndex('status', 'status', { unique: false });
          ramsStore.createIndex('rams_number', 'rams_number', { unique: false });
        }

        // cost_jobs store with status and job_number indexes
        if (!db.objectStoreNames.contains('cost_jobs')) {
          const cjStore = db.createObjectStore('cost_jobs', { keyPath: 'id' });
          cjStore.createIndex('status', 'status', { unique: false });
          cjStore.createIndex('job_number', 'job_number', { unique: true });
        }

        // cost_categories store with job_id index
        if (!db.objectStoreNames.contains('cost_categories')) {
          const ccStore = db.createObjectStore('cost_categories', { keyPath: 'id' });
          ccStore.createIndex('job_id', 'job_id', { unique: false });
        }

        // cost_transactions store with compound indexes for filtering
        if (!db.objectStoreNames.contains('cost_transactions')) {
          const ctStore = db.createObjectStore('cost_transactions', { keyPath: 'id' });
          ctStore.createIndex('job_id', 'job_id', { unique: false });
          ctStore.createIndex('job_date', ['job_id', 'trans_date'], { unique: false });
          ctStore.createIndex('job_category', ['job_id', 'mapped_category'], { unique: false });
          ctStore.createIndex('supplier_name', 'supplier_name', { unique: false });
          ctStore.createIndex('import_batch_id', 'import_batch_id', { unique: false });
        }

        // cost_supplier_mappings store
        if (!db.objectStoreNames.contains('cost_supplier_mappings')) {
          const smStore = db.createObjectStore('cost_supplier_mappings', { keyPath: 'id' });
          smStore.createIndex('job_id', 'job_id', { unique: false });
        }

        // cost_labour_mappings store
        if (!db.objectStoreNames.contains('cost_labour_mappings')) {
          const lmStore = db.createObjectStore('cost_labour_mappings', { keyPath: 'id' });
          lmStore.createIndex('job_id', 'job_id', { unique: false });
          lmStore.createIndex('cost_code', 'cost_code', { unique: false });
        }

        // cost_imports store (import batch records)
        if (!db.objectStoreNames.contains('cost_imports')) {
          db.createObjectStore('cost_imports', { keyPath: 'id' });
        }

        // cost_rate_cards store (NAECI rate cards)
        if (!db.objectStoreNames.contains('cost_rate_cards')) {
          const rcStore = db.createObjectStore('cost_rate_cards', { keyPath: 'id' });
          rcStore.createIndex('is_active', 'is_active', { unique: false });
        }

        // cost_rate_grades store (grades within rate cards)
        if (!db.objectStoreNames.contains('cost_rate_grades')) {
          const rgStore = db.createObjectStore('cost_rate_grades', { keyPath: 'id' });
          rgStore.createIndex('rate_card_id', 'rate_card_id', { unique: false });
        }

        // cost_estimates store
        if (!db.objectStoreNames.contains('cost_estimates')) {
          const ceStore = db.createObjectStore('cost_estimates', { keyPath: 'id' });
          ceStore.createIndex('job_id', 'job_id', { unique: false });
          ceStore.createIndex('status', 'status', { unique: false });
        }

        // cost_estimate_items store
        if (!db.objectStoreNames.contains('cost_estimate_items')) {
          const eiStore = db.createObjectStore('cost_estimate_items', { keyPath: 'id' });
          eiStore.createIndex('estimate_id', 'estimate_id', { unique: false });
          eiStore.createIndex('est_section', ['estimate_id', 'section_number'], { unique: false });
        }

        // Reference data stores (keyed by employee_number)
        if (!db.objectStoreNames.contains('employees')) {
          const empStore = db.createObjectStore('employees', { keyPath: 'employee_number' });
          empStore.createIndex('name', 'name', { unique: false });
          empStore.createIndex('is_active', 'is_active', { unique: false });
        }

        // checklist_state: single object, use a fixed key
        if (!db.objectStoreNames.contains('checklist_state')) {
          db.createObjectStore('checklist_state', { keyPath: '_key' });
        }

        // cost_settings: key-value store for cached export file etc.
        if (!db.objectStoreNames.contains('cost_settings')) {
          db.createObjectStore('cost_settings', { keyPath: 'id' });
        }

        // sync_queue: auto-increment queue
        if (!db.objectStoreNames.contains('sync_queue')) {
          const sq = db.createObjectStore('sync_queue', {
            keyPath: '_qid',
            autoIncrement: true,
          });
          sq.createIndex('entity', 'entity', { unique: false });
          sq.createIndex('status', 'status', { unique: false });
        }
      };

      req.onsuccess = function (e) {
        _db = e.target.result;
        // Auto-migrate localStorage data, then resolve
        migrateFromLocalStorage().then(() => resolve(_db));
      };

      req.onerror = function (e) {
        console.error('MiiDB: IndexedDB open failed', e.target.error);
        reject(e.target.error);
      };
    });

    return _dbReady;
  }

  // ─── Generic IDB helpers ───────────────────────────────────────

  function tx(storeName, mode) {
    return _db.transaction(storeName, mode).objectStore(storeName);
  }

  function idbRequest(request) {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
  }

  // ─── localStorage dual-write helpers ───────────────────────────

  function lsWrite(storeName, data) {
    try {
      const key = STORE_LS_MAP[storeName];
      if (!key) return;
      localStorage.setItem(key, JSON.stringify(data));
    } catch (_) {
      // localStorage full or unavailable — ignore, IndexedDB is primary
    }
  }

  function lsRead(storeName) {
    try {
      const key = STORE_LS_MAP[storeName];
      if (!key) return null;
      const raw = localStorage.getItem(key);
      return raw ? JSON.parse(raw) : null;
    } catch (_) {
      return null;
    }
  }

  // ─── Sync queue ────────────────────────────────────────────────

  async function enqueue(entity, action, record) {
    const store = tx('sync_queue', 'readwrite');
    await idbRequest(
      store.add({
        entity: entity,
        action: action, // 'upsert' | 'delete'
        record_id: record.id || null,
        payload: record,
        status: 'pending',
        queued_at: new Date().toISOString(),
        attempts: 0,
      })
    );
  }

  // ─── Migration from localStorage ──────────────────────────────

  async function migrateFromLocalStorage() {
    const MIGRATED_FLAG = 'mii_idb_migrated';
    if (localStorage.getItem(MIGRATED_FLAG)) return;

    // Migrate array stores
    for (const storeName of ARRAY_STORES) {
      const data = lsRead(storeName);
      if (data && Array.isArray(data) && data.length > 0) {
        const store = tx(storeName, 'readwrite');
        for (const record of data) {
          if (record && record.id) {
            await idbRequest(store.put(record));
          }
        }
      }
    }

    // Migrate checklist_state (object, not array)
    const checklistData = lsRead('checklist_state');
    if (checklistData && typeof checklistData === 'object') {
      const store = tx('checklist_state', 'readwrite');
      await idbRequest(store.put({ _key: 'current', ...checklistData }));
    }

    localStorage.setItem(MIGRATED_FLAG, Date.now().toString());
    console.log('MiiDB: localStorage migration complete');
  }

  // ─── Public API ────────────────────────────────────────────────

  const MiiDB = {
    /**
     * Ensure DB is ready. Call before any operation, or just call
     * the CRUD methods directly — they auto-await readiness.
     */
    ready: openDB,

    /**
     * Save (insert or update) a record in an array store.
     * Also dual-writes to localStorage and enqueues for sync.
     */
    async save(storeName, record) {
      await openDB();
      if (!record.id) throw new Error('MiiDB.save: record must have an id');

      // Write to IndexedDB
      const store = tx(storeName, 'readwrite');
      await idbRequest(store.put(record));

      // Dual-write: rebuild full array in localStorage
      const all = await this.getAll(storeName);
      lsWrite(storeName, all);

      // Enqueue for server sync
      await enqueue(storeName, 'upsert', record);

      return record;
    },

    /**
     * Bulk-save an entire array to a store (replaces all records).
     * Used when existing code does setItem(key, JSON.stringify(fullArray)).
     */
    async saveAll(storeName, records) {
      await openDB();

      // Clear and re-populate
      const store = tx(storeName, 'readwrite');
      await idbRequest(store.clear());
      for (const record of records) {
        if (record && record.id) {
          await idbRequest(
            tx(storeName, 'readwrite').put(record)
          );
        }
      }

      // Dual-write
      lsWrite(storeName, records);

      return records;
    },

    /**
     * Get all records from an array store.
     */
    async getAll(storeName) {
      await openDB();
      const store = tx(storeName, 'readonly');
      return idbRequest(store.getAll());
    },

    /**
     * Get a single record by id.
     */
    async get(storeName, id) {
      await openDB();
      const store = tx(storeName, 'readonly');
      return idbRequest(store.get(id));
    },

    /**
     * Delete a record by id.
     */
    async remove(storeName, id) {
      await openDB();
      const store = tx(storeName, 'readwrite');
      await idbRequest(store.delete(id));

      // Dual-write
      const all = await this.getAll(storeName);
      lsWrite(storeName, all);

      // Enqueue delete for sync
      await enqueue(storeName, 'delete', { id: id });

      return true;
    },

    // ── Checklist-specific (object, not array) ──────────────────

    /**
     * Save the supervisor checklist state object.
     */
    async saveChecklist(stateObj) {
      await openDB();
      const store = tx('checklist_state', 'readwrite');
      await idbRequest(store.put({ _key: 'current', ...stateObj }));

      // Dual-write to localStorage (without _key)
      lsWrite('checklist_state', stateObj);

      return stateObj;
    },

    /**
     * Get the supervisor checklist state object.
     * Returns {} if no data saved yet.
     */
    async getChecklist() {
      await openDB();
      const store = tx('checklist_state', 'readonly');
      const result = await idbRequest(store.get('current'));
      if (!result) return {};
      // Strip the internal _key before returning
      const { _key, ...state } = result;
      return state;
    },

    /**
     * Clear the supervisor checklist.
     */
    async clearChecklist() {
      await openDB();
      const store = tx('checklist_state', 'readwrite');
      await idbRequest(store.clear());
      lsWrite('checklist_state', {});
    },

    // ── Sync queue access ────────────────────────────────────────

    /**
     * Get all pending sync queue items.
     */
    async getSyncQueue() {
      await openDB();
      const store = tx('sync_queue', 'readonly');
      return idbRequest(store.getAll());
    },

    /**
     * Remove a sync queue item after successful sync.
     */
    async removeSyncItem(qid) {
      await openDB();
      const store = tx('sync_queue', 'readwrite');
      await idbRequest(store.delete(qid));
    },

    /**
     * Mark a sync queue item as failed (increment attempts).
     */
    async markSyncFailed(qid) {
      await openDB();
      const store = tx('sync_queue', 'readonly');
      const item = await idbRequest(store.get(qid));
      if (item) {
        item.attempts += 1;
        item.status = 'failed';
        item.last_attempt = new Date().toISOString();
        const ws = tx('sync_queue', 'readwrite');
        await idbRequest(ws.put(item));
      }
    },

    /**
     * Get count of pending sync items (for status indicator).
     */
    async getSyncPendingCount() {
      await openDB();
      const all = await idbRequest(tx('sync_queue', 'readonly').getAll());
      return all.filter((i) => i.status === 'pending' || i.status === 'failed')
        .length;
    },

    // ── Employee roster (reference data, pulled from server) ────

    /**
     * Get all cached employees. Returns array of
     * { employee_number, name, trade, is_active }.
     * If activeOnly is true (default), returns only active employees.
     */
    async getEmployees(activeOnly = true) {
      await openDB();
      const store = tx('employees', 'readonly');
      const all = await idbRequest(store.getAll());
      if (activeOnly) return all.filter((e) => e.is_active !== false);
      return all;
    },

    /**
     * Get employees as [id, name] pairs for dropdown compatibility.
     * Sorted alphabetically by name.
     */
    async getEmployeePairs(activeOnly = true) {
      const emps = await this.getEmployees(activeOnly);
      return emps
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((e) => [e.employee_number, e.name]);
    },

    /**
     * Sync employees from the server API.
     * Replaces the entire local cache with fresh data.
     * @param {string} apiBase - e.g. 'https://mii-hub-api.azurewebsites.net/api'
     * @returns {number} count of employees cached
     */
    async syncEmployees(apiBase) {
      await openDB();
      try {
        const url = apiBase.replace(/\/+$/, '') + '/employees';
        const resp = await fetch(url);
        if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
        const data = await resp.json();
        const employees = data.employees || data;

        // Clear and re-populate
        const store = tx('employees', 'readwrite');
        await idbRequest(store.clear());
        for (const emp of employees) {
          await idbRequest(tx('employees', 'readwrite').put(emp));
        }

        // Store last sync timestamp
        localStorage.setItem('mii_employees_synced', new Date().toISOString());
        console.log(`MiiDB: synced ${employees.length} employees`);
        return employees.length;
      } catch (err) {
        console.warn('MiiDB: employee sync failed, using cached data', err);
        return -1;
      }
    },

    /**
     * Seed the employees store from a JSON array (e.g. loaded from employees.json).
     * Only writes if the store is currently empty.
     */
    async seedEmployees(employeesArray) {
      await openDB();
      const existing = await idbRequest(tx('employees', 'readonly').count());
      if (existing > 0) return; // Already seeded

      const store = tx('employees', 'readwrite');
      for (const emp of employeesArray) {
        if (emp.employee_number) {
          await idbRequest(tx('employees', 'readwrite').put(emp));
        }
      }
      console.log(`MiiDB: seeded ${employeesArray.length} employees into IndexedDB`);
    },

    /**
     * Check if employees have been synced recently (within hours).
     * @param {number} maxAgeHours - max age before considered stale (default 24)
     */
    isEmployeeCacheStale(maxAgeHours = 24) {
      const last = localStorage.getItem('mii_employees_synced');
      if (!last) return true;
      const age = Date.now() - new Date(last).getTime();
      return age > maxAgeHours * 60 * 60 * 1000;
    },

    // ── Utilities ────────────────────────────────────────────────

    /**
     * Helper: read from localStorage (for code that still needs it during migration).
     * Prefer MiiDB.getAll() instead.
     */
    lsGet(storeName) {
      return lsRead(storeName);
    },

    /**
     * Seed-flag helpers (thin wrappers — seeds are localStorage-only).
     */
    isSeeded(flagKey) {
      return !!localStorage.getItem(flagKey);
    },

    markSeeded(flagKey) {
      localStorage.setItem(flagKey, 'true');
    },
  };

  // ── Compatibility shim ──────────────────────────────────────────
  // Drop-in helpers so existing code can transition gradually.
  // Instead of: JSON.parse(localStorage.getItem('mii_defects') || '[]')
  // Use:        await MiiDB.getAll('defects')

  // Expose globally
  global.MiiDB = MiiDB;

  // Auto-open on load so the DB is ready when forms need it
  if (typeof document !== 'undefined') {
    openDB().catch((err) =>
      console.error('MiiDB: failed to initialise IndexedDB', err)
    );
  }
})(typeof self !== 'undefined' ? self : this);
