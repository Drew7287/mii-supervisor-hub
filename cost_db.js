/**
 * CostDB - Cost Tracker data layer for MII Supervisor Hub
 *
 * Provides all cost tracking operations on top of MiiDB IndexedDB stores.
 * Port of the PySide6/SQLAlchemy desktop app to browser-based IndexedDB.
 *
 * Dependencies: mii-db.js must be loaded first (provides MiiDB).
 *
 * Usage:
 *   await CostDB.createJob({ job_number: 'SW4455', ... });
 *   const analysis = await CostDB.getFullAnalysis(jobId);
 */
(function (global) {
  'use strict';

  // ── Constants ──────────────────────────────────────────────────

  const DEFAULT_CATEGORIES = [
    { number: 1, name: 'Design' },
    { number: 2, name: 'Labour' },
    { number: 3, name: 'Plant & Equipment' },
    { number: 4, name: 'Equipment Hired' },
    { number: 5, name: 'Materials' },
    { number: 6, name: 'Off Site' },
    { number: 7, name: 'Consumables' },
    { number: 8, name: 'Travel & Accommodation' },
  ];

  // ── Helpers ────────────────────────────────────────────────────

  function generateId() {
    return crypto.randomUUID();
  }

  /**
   * Format a number as GBP currency string.
   * @param {number} value
   * @param {number} decimals - 0 for whole pounds, 2 for pence
   * @returns {string} e.g. "£12,345" or "£12,345.67"
   */
  function formatGBP(value, decimals = 0) {
    const num = parseFloat(value) || 0;
    const abs = Math.abs(num);
    const formatted = abs.toLocaleString('en-GB', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
    return num < 0 ? `-\u00A3${formatted}` : `\u00A3${formatted}`;
  }

  /**
   * Format as GBP with explicit + or - sign.
   * @param {number} value
   * @param {number} decimals
   * @returns {string} e.g. "+£1,234" or "-£567"
   */
  function formatGBPSigned(value, decimals = 0) {
    const num = parseFloat(value) || 0;
    const abs = Math.abs(num);
    const formatted = abs.toLocaleString('en-GB', {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
    if (num > 0) return `+\u00A3${formatted}`;
    if (num < 0) return `-\u00A3${formatted}`;
    return `\u00A3${formatted}`;
  }

  /**
   * Calculate percentage, safe against division by zero.
   * @param {number} numerator
   * @param {number} denominator
   * @returns {number} percentage value (e.g. 75.5)
   */
  function pctOf(numerator, denominator) {
    if (!denominator || denominator === 0) return 0;
    return (numerator / denominator) * 100;
  }

  /**
   * Get the Monday of the week containing the given date string.
   * @param {string} dateStr - ISO date string (YYYY-MM-DD)
   * @returns {string} ISO date string for the Monday
   */
  function getWeekCommencing(dateStr) {
    const d = new Date(dateStr + 'T00:00:00');
    const day = d.getDay(); // 0=Sun, 1=Mon, ...
    const diff = day === 0 ? -6 : 1 - day; // adjust to Monday
    d.setDate(d.getDate() + diff);
    return d.toISOString().slice(0, 10);
  }

  /**
   * Get YYYY-MM from a date string.
   * @param {string} dateStr - ISO date string (YYYY-MM-DD)
   * @returns {string} e.g. "2026-03"
   */
  function getYearMonth(dateStr) {
    return (dateStr || '').slice(0, 7);
  }

  // ── Internal DB access helpers ─────────────────────────────────

  /**
   * Get all records from a cost store, optionally filtered by index.
   * @param {string} storeName
   * @param {string} [indexName] - index to query
   * @param {*} [indexValue] - value to match on the index
   * @returns {Promise<Array>}
   */
  async function getAllFromStore(storeName, indexName, indexValue) {
    await MiiDB.ready();
    if (indexName !== undefined && indexValue !== undefined) {
      // Use index query via raw IDB transaction
      return new Promise((resolve, reject) => {
        const db = MiiDB._getDB ? MiiDB._getDB() : null;
        // Fall back to getAll + filter if _getDB not available
        MiiDB.getAll(storeName).then(records => {
          resolve(records.filter(r => {
            const val = r[indexName];
            if (Array.isArray(indexValue)) {
              return JSON.stringify(indexValue) === JSON.stringify(
                indexValue.map((_, i) => {
                  const keys = indexName === 'job_date' ? ['job_id', 'trans_date'] :
                    indexName === 'job_category' ? ['job_id', 'mapped_category'] : [];
                  return r[keys[i]];
                })
              );
            }
            return val === indexValue;
          }));
        }).catch(reject);
      });
    }
    return MiiDB.getAll(storeName);
  }

  /**
   * Get records from a store filtered by a simple field value.
   * More straightforward than index-based queries for our use case.
   */
  async function getByField(storeName, field, value) {
    const all = await MiiDB.getAll(storeName);
    return all.filter(r => r[field] === value);
  }

  /**
   * Get records matching multiple field conditions.
   */
  async function getByFields(storeName, conditions) {
    const all = await MiiDB.getAll(storeName);
    return all.filter(r => {
      for (const [field, value] of Object.entries(conditions)) {
        if (r[field] !== value) return false;
      }
      return true;
    });
  }

  // ── Job CRUD ───────────────────────────────────────────────────

  const CostDB = {

    /**
     * Create a new job with its 8 cost categories.
     * @param {Object} params
     * @param {string} params.job_number - e.g. 'SW4455'
     * @param {string} params.job_name - e.g. 'Hirwaun Power Station'
     * @param {string} [params.client] - client name
     * @param {number} params.markup - markup percentage (e.g. 14 for 14%)
     * @param {Array} params.categories - array of {number, contract_value, name?, zero_budget?}
     * @param {string} [params.notes]
     * @param {string} [params.estimate_ref]
     * @param {string} [params.ai_notes]
     * @param {string} [params.group_id] - parent group id
     * @returns {Promise<string>} job id
     */
    async createJob({ job_number, job_name, client = '', markup = 14, categories = [], notes = '',
                       estimate_ref = '', ai_notes = '', group_id = null }) {
      await MiiDB.ready();

      const jobId = generateId();
      const now = new Date().toISOString();
      const markupPct = parseFloat(markup) || 14;
      const markupMultiplier = 1 + markupPct / 100;

      const job = {
        id: jobId,
        job_number,
        job_name,
        client,
        markup_pct: markupPct,
        notes,
        estimate_ref,
        ai_notes,
        status: 'active',
        is_group: false,
        group_id,
        last_import_at: null,
        created_at: now,
        updated_at: now,
      };

      await MiiDB.save('cost_jobs', job);

      // Create 8 categories with contract values
      const catLookup = {};
      const catNameLookup = {};
      const catZeroBudget = {};
      for (const c of categories) {
        catLookup[c.number] = parseFloat(c.contract_value) || 0;
        if (c.name) catNameLookup[c.number] = c.name;
        if (c.zero_budget) catZeroBudget[c.number] = true;
      }

      let totalContract = 0;
      let totalAtCost = 0;

      for (const def of DEFAULT_CATEGORIES) {
        const contractValue = catLookup[def.number] || 0;
        const atCost = markupMultiplier > 0 ? contractValue / markupMultiplier : 0;
        totalContract += contractValue;
        totalAtCost += atCost;

        const cat = {
          id: generateId(),
          job_id: jobId,
          category_number: def.number,
          category_name: catNameLookup[def.number] || def.name,
          contract_value: contractValue,
          at_cost: atCost,
          zero_budget: catZeroBudget[def.number] || false,
          sort_order: def.number,
          created_at: now,
        };
        await MiiDB.save('cost_categories', cat);
      }

      // Store rolled-up totals on job
      job.contract_value_total = totalContract;
      job.budget_at_cost = totalAtCost;
      await MiiDB.save('cost_jobs', job);

      // Fire-and-forget server sync
      this.syncJobToServer(jobId).catch(() => {});

      return jobId;
    },

    /**
     * Create a job group from an existing primary job.
     * @param {string} groupName - display name for the group
     * @param {string} primaryJobId - the first child job
     * @returns {Promise<string>} group id
     */
    async createGroup(groupName, primaryJobId) {
      await MiiDB.ready();
      const primary = await MiiDB.get('cost_jobs', primaryJobId);
      if (!primary) throw new Error('Primary job not found');

      const groupId = generateId();
      const now = new Date().toISOString();

      // Copy categories from primary job
      const primaryCats = await getByField('cost_categories', 'job_id', primaryJobId);

      const group = {
        id: groupId,
        job_number: primary.job_number,
        job_name: groupName,
        client: primary.client || '',
        markup_pct: primary.markup_pct || 14,
        notes: '',
        estimate_ref: '',
        ai_notes: '',
        status: 'active',
        is_group: true,
        group_id: null,
        last_import_at: null,
        created_at: now,
        updated_at: now,
        contract_value_total: primary.contract_value_total || 0,
        budget_at_cost: primary.budget_at_cost || 0,
      };
      await MiiDB.save('cost_jobs', group);

      // Create group-level categories (copied from primary)
      for (const pc of primaryCats) {
        await MiiDB.save('cost_categories', {
          id: generateId(),
          job_id: groupId,
          category_number: pc.category_number,
          category_name: pc.category_name,
          contract_value: pc.contract_value,
          at_cost: pc.at_cost,
          zero_budget: pc.zero_budget || false,
          sort_order: pc.sort_order || pc.category_number,
          created_at: now,
        });
      }

      // Link primary job as child
      primary.group_id = groupId;
      primary.updated_at = now;
      await MiiDB.save('cost_jobs', primary);

      return groupId;
    },

    /**
     * Add a job to an existing group.
     * @param {string} groupId
     * @param {string} childJobId
     */
    async addJobToGroup(groupId, childJobId) {
      await MiiDB.ready();
      const child = await MiiDB.get('cost_jobs', childJobId);
      if (!child) throw new Error('Child job not found');
      child.group_id = groupId;
      child.updated_at = new Date().toISOString();
      await MiiDB.save('cost_jobs', child);
    },

    /**
     * Get child job IDs for a group.
     * @param {string} groupId
     * @returns {Promise<Array<string>>}
     */
    async getGroupChildIds(groupId) {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_jobs');
      return all.filter(j => j.group_id === groupId).map(j => j.id);
    },

    /**
     * General-purpose job update.
     * @param {string} jobId
     * @param {Object} fields - fields to update
     */
    async updateJob(jobId, fields) {
      await MiiDB.ready();
      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) throw new Error(`Job ${jobId} not found`);
      Object.assign(job, fields, { updated_at: new Date().toISOString() });
      await MiiDB.save('cost_jobs', job);
      this.syncJobToServer(jobId).catch(() => {});
    },

    /**
     * Get a job with its categories and computed financials.
     * For groups, merges financials across all child jobs.
     * @param {string} jobId
     * @returns {Promise<Object|null>}
     */
    async getJob(jobId) {
      await MiiDB.ready();

      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) return null;

      const categories = await getByField('cost_categories', 'job_id', jobId);

      // For groups, aggregate financials from all children
      let financials;
      if (job.is_group) {
        const childIds = await this.getGroupChildIds(jobId);
        financials = await this._getCategoryFinancialsMulti(childIds);
      } else {
        financials = await this.getCategoryFinancials(jobId);
      }

      // Merge financials into categories
      const finMap = {};
      for (const f of financials) {
        finMap[f.category_number] = f;
      }

      job.categories = categories
        .sort((a, b) => (a.sort_order || a.category_number) - (b.sort_order || b.category_number))
        .map(cat => {
          const fin = finMap[cat.category_number] || { actual: 0, committed: 0, exposure: 0, count: 0 };
          return {
            ...cat,
            actual: fin.actual,
            committed: fin.committed,
            exposure: fin.exposure,
            variance: (parseFloat(cat.at_cost) || 0) - fin.exposure,
            count: fin.count,
          };
        });

      // Job-level totals
      job.total_contract = categories.reduce((s, c) => s + (parseFloat(c.contract_value) || 0), 0);
      job.total_at_cost = categories.reduce((s, c) => s + (parseFloat(c.at_cost) || 0), 0);
      job.total_actual = job.categories.reduce((s, c) => s + c.actual, 0);
      job.total_committed = job.categories.reduce((s, c) => s + c.committed, 0);
      job.total_exposure = job.categories.reduce((s, c) => s + c.exposure, 0);
      job.total_variance = job.total_at_cost - job.total_exposure;

      // Revenue total
      let revTxns;
      if (job.is_group) {
        const childIds = await this.getGroupChildIds(jobId);
        revTxns = [];
        for (const cid of childIds) {
          revTxns.push(...await this.getRevenueTransactions(cid));
        }
      } else {
        revTxns = await this.getRevenueTransactions(jobId);
      }
      job.total_revenue = revTxns.reduce((s, t) => s + (parseFloat(t.total_cost) || 0), 0);

      // Transaction count
      if (job.is_group) {
        const childIds = await this.getGroupChildIds(jobId);
        let count = 0;
        for (const cid of childIds) {
          const txns = await getByField('cost_transactions', 'job_id', cid);
          count += txns.length;
        }
        job.transaction_count = count;
      } else {
        const txns = await getByField('cost_transactions', 'job_id', jobId);
        job.transaction_count = txns.length;
      }

      return job;
    },

    /**
     * Get all jobs for the portfolio dashboard.
     * Hides children that belong to a group; shows group rows instead.
     * @returns {Promise<Array>}
     */
    async getAllJobs() {
      await MiiDB.ready();
      const jobs = await MiiDB.getAll('cost_jobs');
      // Only show standalone jobs and groups (hide children)
      const visible = jobs.filter(j => !j.group_id);
      const enriched = [];
      for (const job of visible) {
        const full = await this.getJob(job.id);
        if (full) enriched.push(full);
      }
      return enriched.sort((a, b) => (a.job_number || '').localeCompare(b.job_number || ''));
    },

    /**
     * Update markup for a job and recalculate all category at_cost values.
     * @param {string} jobId
     * @param {number} markupPct - markup percentage (e.g. 14 for 14%)
     */
    async updateMarkup(jobId, markupPct) {
      await MiiDB.ready();

      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) throw new Error(`Job ${jobId} not found`);

      const pct = parseFloat(markupPct) || 14;
      const multiplier = 1 + pct / 100;
      job.markup_pct = pct;
      job.updated_at = new Date().toISOString();

      // Recalculate at_cost for every category
      const categories = await getByField('cost_categories', 'job_id', jobId);
      let totalContract = 0;
      let totalAtCost = 0;
      for (const cat of categories) {
        cat.at_cost = multiplier > 0 ? (parseFloat(cat.contract_value) || 0) / multiplier : 0;
        totalContract += parseFloat(cat.contract_value) || 0;
        totalAtCost += cat.at_cost;
        await MiiDB.save('cost_categories', cat);
      }

      job.contract_value_total = totalContract;
      job.budget_at_cost = totalAtCost;
      await MiiDB.save('cost_jobs', job);
      this.syncJobToServer(jobId).catch(() => {});
    },

    /**
     * Update job status (active, complete, archived).
     * @param {string} jobId
     * @param {string} status
     */
    async updateJobStatus(jobId, status) {
      await MiiDB.ready();
      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) throw new Error(`Job ${jobId} not found`);
      job.status = status;
      job.updated_at = new Date().toISOString();
      await MiiDB.save('cost_jobs', job);
      this.syncJobToServer(jobId).catch(() => {});
    },

    /**
     * Delete a job and all related data (categories, transactions, mappings).
     * @param {string} jobId
     */
    async deleteJob(jobId) {
      await MiiDB.ready();

      // Delete categories
      const cats = await getByField('cost_categories', 'job_id', jobId);
      for (const c of cats) await MiiDB.remove('cost_categories', c.id);

      // Delete transactions
      const txns = await getByField('cost_transactions', 'job_id', jobId);
      for (const t of txns) await MiiDB.remove('cost_transactions', t.id);

      // Delete supplier mappings
      const sms = await getByField('cost_supplier_mappings', 'job_id', jobId);
      for (const m of sms) await MiiDB.remove('cost_supplier_mappings', m.id);

      // Delete labour mappings
      const lms = await getByField('cost_labour_mappings', 'job_id', jobId);
      for (const m of lms) await MiiDB.remove('cost_labour_mappings', m.id);

      // Delete the job itself
      await MiiDB.remove('cost_jobs', jobId);
    },

    // ── Category Financials ────────────────────────────────────────

    /**
     * Compute financial summary per category from transactions.
     * @param {string} jobId
     * @returns {Promise<Array>} array of {category_number, actual, committed, exposure, count}
     */
    async getCategoryFinancials(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      // Initialise buckets for categories 0-8 (0 = unmapped)
      const buckets = {};
      for (let i = 0; i <= 8; i++) {
        buckets[i] = { category_number: i, actual: 0, committed: 0, exposure: 0, count: 0 };
      }

      for (const t of txns) {
        const cat = t.mapped_category || 0;
        if (!buckets[cat]) {
          buckets[cat] = { category_number: cat, actual: 0, committed: 0, exposure: 0, count: 0 };
        }
        const cost = parseFloat(t.total_cost) || 0;
        buckets[cat].count++;

        if (t.is_revenue) continue; // Revenue doesn't count toward category spend

        if (t.is_commitment) {
          buckets[cat].committed += cost;
        } else {
          buckets[cat].actual += cost;
        }
      }

      // Calculate exposure for each
      for (const b of Object.values(buckets)) {
        b.exposure = b.actual + b.committed;
      }

      return Object.values(buckets).sort((a, b) => a.category_number - b.category_number);
    },

    // ── Transaction Operations ─────────────────────────────────────

    /**
     * Bulk insert transactions for a job.
     * @param {string} jobId
     * @param {Array} transactions - array of transaction objects
     * @param {string} importBatchId - the import batch these belong to
     * @returns {Promise<number>} count inserted
     */
    async importTransactions(jobId, transactions, importBatchId) {
      await MiiDB.ready();
      let count = 0;
      for (const t of transactions) {
        const record = {
          ...t,
          id: t.id || generateId(),
          job_id: jobId,
          import_batch_id: importBatchId,
          mapped_category: t.mapped_category || 0,
          mapping_source: t.mapping_source || 'unmapped',
          mapping_confidence: t.mapping_confidence || 'low',
          is_commitment: t.is_commitment || false,
          is_revenue: t.is_revenue || false,
          total_cost: parseFloat(t.total_cost) || 0,
        };
        await MiiDB.save('cost_transactions', record);
        count++;
      }
      return count;
    },

    /**
     * Get transactions for a job, optionally filtered.
     * @param {string} jobId
     * @param {Object} [filters]
     * @param {number} [filters.category] - mapped_category number
     * @param {string} [filters.transType] - trans_type value
     * @returns {Promise<Array>}
     */
    async getTransactions(jobId, { category, transType } = {}) {
      await MiiDB.ready();
      let txns = await getByField('cost_transactions', 'job_id', jobId);

      if (category !== undefined) {
        txns = txns.filter(t => t.mapped_category === category);
      }
      if (transType !== undefined) {
        txns = txns.filter(t => t.trans_type === transType);
      }

      return txns.sort((a, b) => (a.trans_date || '').localeCompare(b.trans_date || ''));
    },

    /**
     * Get transactions that have not been mapped to a category.
     * @param {string} jobId
     * @returns {Promise<Array>}
     */
    async getUnmappedTransactions(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);
      return txns.filter(t => (t.mapped_category || 0) === 0 && !t.is_revenue);
    },

    /**
     * Manually map a transaction to a category.
     * @param {string} txnId
     * @param {number} categoryNumber - 1-8
     */
    async manuallyMapTransaction(txnId, categoryNumber) {
      await MiiDB.ready();
      const txn = await MiiDB.get('cost_transactions', txnId);
      if (!txn) throw new Error(`Transaction ${txnId} not found`);

      txn.mapped_category = categoryNumber;
      txn.mapping_source = 'manual';
      txn.mapping_confidence = 'high';
      await MiiDB.save('cost_transactions', txn);
    },

    /**
     * Delete all transactions within a date range for a job.
     * Used for date-range replace on reimport.
     * @param {string} jobId
     * @param {string} startDate - ISO date (YYYY-MM-DD)
     * @param {string} endDate - ISO date (YYYY-MM-DD)
     * @returns {Promise<number>} count deleted
     */
    async deleteTransactionsInDateRange(jobId, startDate, endDate) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);
      let count = 0;

      for (const t of txns) {
        const d = t.trans_date || '';
        if (d >= startDate && d <= endDate) {
          await MiiDB.remove('cost_transactions', t.id);
          count++;
        }
      }

      return count;
    },

    // ── Mapping Rules ──────────────────────────────────────────────

    /**
     * Create a supplier mapping rule.
     * @param {Object} params
     * @returns {Promise<string>} mapping id
     */
    async createSupplierMapping({ supplier_prefix, cost_code_prefix = '', category_number, note = '', job_id = null }) {
      await MiiDB.ready();
      const id = generateId();
      const record = {
        id,
        supplier_prefix: (supplier_prefix || '').toUpperCase(),
        cost_code_prefix: (cost_code_prefix || '').toUpperCase(),
        category_number,
        note,
        job_id,
        priority: job_id ? 100 : 50, // Job-specific rules take priority
        created_at: new Date().toISOString(),
      };
      await MiiDB.save('cost_supplier_mappings', record);
      return id;
    },

    /**
     * Get supplier mappings: global (job_id=null) + job-specific, sorted by priority desc.
     * @param {string} jobId
     * @returns {Promise<Array>}
     */
    async getSupplierMappings(jobId) {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_supplier_mappings');
      return all
        .filter(m => m.job_id === null || m.job_id === jobId)
        .sort((a, b) => (b.priority || 0) - (a.priority || 0));
    },

    /**
     * Delete a supplier mapping rule.
     * @param {string} id
     */
    async deleteSupplierMapping(id) {
      await MiiDB.ready();
      await MiiDB.remove('cost_supplier_mappings', id);
    },

    /**
     * Create a labour mapping rule.
     * @param {Object} params
     * @returns {Promise<string>} mapping id
     */
    async createLabourMapping({ cost_code, category_number, note = '', job_id = null }) {
      await MiiDB.ready();
      const id = generateId();
      const record = {
        id,
        cost_code: (cost_code || '').toUpperCase(),
        category_number,
        note,
        job_id,
        priority: job_id ? 100 : 50,
        created_at: new Date().toISOString(),
      };
      await MiiDB.save('cost_labour_mappings', record);
      return id;
    },

    /**
     * Get labour mappings: global (job_id=null) + job-specific, sorted by priority desc.
     * @param {string} jobId
     * @returns {Promise<Array>}
     */
    async getLabourMappings(jobId) {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_labour_mappings');
      return all
        .filter(m => m.job_id === null || m.job_id === jobId)
        .sort((a, b) => (b.priority || 0) - (a.priority || 0));
    },

    /**
     * Delete a labour mapping rule.
     * @param {string} id
     */
    async deleteLabourMapping(id) {
      await MiiDB.ready();
      await MiiDB.remove('cost_labour_mappings', id);
    },

    // ── Mapping Engine ─────────────────────────────────────────────

    /**
     * Re-map ALL transactions for a job using current mapping rules.
     * Skips manually-mapped transactions.
     *
     * @param {string} jobId
     * @returns {Promise<Object>} {mapped, unmapped, revenue, commitment}
     */
    async applyMappings(jobId) {
      await MiiDB.ready();

      // Load mapping rules (global + job-specific, sorted by priority desc)
      const supplierMappings = await this.getSupplierMappings(jobId);
      const labourMappings = await this.getLabourMappings(jobId);

      // Load all transactions for job
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const counts = { mapped: 0, unmapped: 0, revenue: 0, commitment: 0 };

      for (const t of txns) {
        // Skip manually-mapped transactions
        if (t.mapping_source === 'manual') {
          counts.mapped++;
          continue;
        }

        // Revenue detection: cost_code Z99
        if ((t.cost_code || '').toUpperCase() === 'Z99') {
          t.is_revenue = true;
          t.mapped_category = 0;
          t.mapping_source = 'auto_revenue';
          t.mapping_confidence = 'high';
          counts.revenue++;
          await MiiDB.save('cost_transactions', t);
          continue;
        }

        // Commitment detection: PO, CO, or type 6
        const trtype = (t.jw_trtype || '').toUpperCase();
        if (trtype === 'PO' || trtype === 'CO' || trtype === '6') {
          t.is_commitment = true;
        } else {
          t.is_commitment = false;
        }

        let matched = false;
        const costCodeUpper = (t.cost_code || '').toUpperCase();
        const supplierUpper = (t.supplier_name || '').toUpperCase();
        const costCodeDescUpper = (t.cost_code_desc || '').toUpperCase();

        // Try labour match first: exact cost_code match
        for (const lm of labourMappings) {
          if (costCodeUpper === lm.cost_code) {
            t.mapped_category = lm.category_number;
            t.mapping_source = 'labour_rule';
            t.mapping_confidence = lm.job_id ? 'high' : 'medium';
            matched = true;
            break;
          }
        }

        // Try supplier match: supplier_name starts with prefix AND cost_code_desc starts with prefix
        if (!matched) {
          for (const sm of supplierMappings) {
            const supplierMatch = !sm.supplier_prefix || supplierUpper.startsWith(sm.supplier_prefix);
            const codeMatch = !sm.cost_code_prefix || costCodeDescUpper.startsWith(sm.cost_code_prefix);

            if (supplierMatch && codeMatch && (sm.supplier_prefix || sm.cost_code_prefix)) {
              t.mapped_category = sm.category_number;
              t.mapping_source = 'supplier_rule';
              t.mapping_confidence = sm.job_id ? 'high' : 'medium';
              matched = true;
              break;
            }
          }
        }

        // No match
        if (!matched) {
          t.mapped_category = 0;
          t.mapping_source = 'unmapped';
          t.mapping_confidence = 'low';
          counts.unmapped++;
        } else {
          counts.mapped++;
        }

        if (t.is_commitment) counts.commitment++;

        await MiiDB.save('cost_transactions', t);
      }

      return counts;
    },

    // ── Multi-Job Aggregation (for groups) ───────────────────────

    /**
     * Category financials merged across multiple jobs.
     * @param {Array<string>} jobIds
     * @returns {Promise<Array>}
     */
    async _getCategoryFinancialsMulti(jobIds) {
      const merged = {};
      for (const jid of jobIds) {
        const fins = await this.getCategoryFinancials(jid);
        for (const f of fins) {
          if (!merged[f.category_number]) {
            merged[f.category_number] = { category_number: f.category_number, actual: 0, committed: 0, exposure: 0, count: 0 };
          }
          merged[f.category_number].actual += f.actual;
          merged[f.category_number].committed += f.committed;
          merged[f.category_number].exposure += f.exposure;
          merged[f.category_number].count += f.count;
        }
      }
      return Object.values(merged).sort((a, b) => a.category_number - b.category_number);
    },

    /**
     * Get transactions for multiple jobs (for group views).
     * @param {Array<string>} jobIds
     * @returns {Promise<Array>}
     */
    async _getTransactionsMulti(jobIds) {
      let all = [];
      for (const jid of jobIds) {
        const txns = await getByField('cost_transactions', 'job_id', jid);
        all = all.concat(txns);
      }
      return all.sort((a, b) => (a.trans_date || '').localeCompare(b.trans_date || ''));
    },

    // ── Mapping Engine Enhancements ───────────────────────────────

    /**
     * Upsert a supplier mapping (create or update if match exists).
     * @param {Object} params
     * @returns {Promise<string>} mapping id
     */
    async upsertSupplierMapping({ supplier_prefix, cost_code_prefix = '', category_number, note = '', job_id = null }) {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_supplier_mappings');
      const spUpper = (supplier_prefix || '').toUpperCase();
      const ccUpper = (cost_code_prefix || '').toUpperCase();
      const existing = all.find(m =>
        (m.supplier_prefix || '').toUpperCase() === spUpper &&
        (m.cost_code_prefix || '').toUpperCase() === ccUpper &&
        m.job_id === job_id
      );

      if (existing) {
        existing.category_number = category_number;
        if (note) existing.note = note;
        await MiiDB.save('cost_supplier_mappings', existing);
        return existing.id;
      }

      return this.createSupplierMapping({ supplier_prefix, cost_code_prefix, category_number, note, job_id });
    },

    /**
     * Upsert a labour mapping (create or update if match exists).
     */
    async upsertLabourMapping({ cost_code, category_number, note = '', job_id = null }) {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_labour_mappings');
      const ccUpper = (cost_code || '').toUpperCase();
      const existing = all.find(m =>
        (m.cost_code || '').toUpperCase() === ccUpper && m.job_id === job_id
      );

      if (existing) {
        existing.category_number = category_number;
        if (note) existing.note = note;
        await MiiDB.save('cost_labour_mappings', existing);
        return existing.id;
      }

      return this.createLabourMapping({ cost_code, category_number, note, job_id });
    },

    /**
     * Promote a job-specific supplier mapping to global (job_id=null).
     * @param {string} mappingId
     */
    async promoteSupplierToGlobal(mappingId) {
      await MiiDB.ready();
      const m = await MiiDB.get('cost_supplier_mappings', mappingId);
      if (!m) throw new Error('Mapping not found');
      // Create global copy
      await this.upsertSupplierMapping({
        supplier_prefix: m.supplier_prefix,
        cost_code_prefix: m.cost_code_prefix,
        category_number: m.category_number,
        note: m.note,
        job_id: null,
      });
    },

    /**
     * Promote a job-specific labour mapping to global.
     * @param {string} mappingId
     */
    async promoteLabourToGlobal(mappingId) {
      await MiiDB.ready();
      const m = await MiiDB.get('cost_labour_mappings', mappingId);
      if (!m) throw new Error('Mapping not found');
      await this.upsertLabourMapping({
        cost_code: m.cost_code,
        category_number: m.category_number,
        note: m.note,
        job_id: null,
      });
    },

    /**
     * Get ALL supplier mappings across all jobs (for mapping library).
     * @returns {Promise<Array>}
     */
    async getAllSupplierMappings() {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_supplier_mappings');
      // Enrich with job_number
      const jobs = await MiiDB.getAll('cost_jobs');
      const jobMap = {};
      for (const j of jobs) jobMap[j.id] = j.job_number;
      return all.map(m => ({
        ...m,
        job_number: m.job_id ? (jobMap[m.job_id] || '') : '(Global)',
      })).sort((a, b) => (b.priority || 0) - (a.priority || 0));
    },

    /**
     * Get ALL labour mappings across all jobs (for mapping library).
     * @returns {Promise<Array>}
     */
    async getAllLabourMappings() {
      await MiiDB.ready();
      const all = await MiiDB.getAll('cost_labour_mappings');
      const jobs = await MiiDB.getAll('cost_jobs');
      const jobMap = {};
      for (const j of jobs) jobMap[j.id] = j.job_number;
      return all.map(m => ({
        ...m,
        job_number: m.job_id ? (jobMap[m.job_id] || '') : '(Global)',
      })).sort((a, b) => (b.priority || 0) - (a.priority || 0));
    },

    /**
     * Re-apply mappings to ALL active jobs (global rules update).
     * Skips group rows (they have no transactions).
     * @returns {Promise<Object>} {jobs_processed, total_mapped, total_unmapped}
     */
    async applyGlobalToAllJobs() {
      await MiiDB.ready();
      const jobs = await MiiDB.getAll('cost_jobs');
      const active = jobs.filter(j => j.status === 'active' && !j.is_group);
      let totalMapped = 0, totalUnmapped = 0;

      for (const job of active) {
        const result = await this.applyMappings(job.id);
        totalMapped += result.mapped;
        totalUnmapped += result.unmapped;
      }

      return { jobs_processed: active.length, total_mapped: totalMapped, total_unmapped: totalUnmapped };
    },

    /**
     * Copy mapping rules from one job to another.
     * @param {string} sourceJobId
     * @param {string} targetJobId
     */
    async copyMappings(sourceJobId, targetJobId) {
      await MiiDB.ready();
      const supplierMaps = await getByField('cost_supplier_mappings', 'job_id', sourceJobId);
      const labourMaps = await getByField('cost_labour_mappings', 'job_id', sourceJobId);

      for (const sm of supplierMaps) {
        await this.createSupplierMapping({
          supplier_prefix: sm.supplier_prefix,
          cost_code_prefix: sm.cost_code_prefix,
          category_number: sm.category_number,
          note: sm.note,
          job_id: targetJobId,
        });
      }

      for (const lm of labourMaps) {
        await this.createLabourMapping({
          cost_code: lm.cost_code,
          category_number: lm.category_number,
          note: lm.note,
          job_id: targetJobId,
        });
      }
    },

    // ── Activity Classification ────────────────────────────────────

    /**
     * Classify all jobs based on latest transaction date.
     * ACTIVE_THRESHOLD = 90 days, DORMANT_THRESHOLD = 365 days.
     * Skips jobs with status 'complete' or 'on_hold'.
     * @returns {Promise<Object>} {active, dormant, archived, unchanged}
     */
    async classifyJobs() {
      await MiiDB.ready();
      const jobs = await MiiDB.getAll('cost_jobs');
      const now = Date.now();
      const ACTIVE_MS = 90 * 86400000;
      const DORMANT_MS = 365 * 86400000;
      const counts = { active: 0, dormant: 0, archived: 0, unchanged: 0 };

      for (const job of jobs) {
        if (job.is_group || job.status === 'complete' || job.status === 'on_hold') {
          counts.unchanged++;
          continue;
        }

        // Find latest transaction date
        const txns = await getByField('cost_transactions', 'job_id', job.id);
        const dates = txns.map(t => t.trans_date).filter(Boolean).sort();
        const latest = dates.length > 0 ? new Date(dates[dates.length - 1] + 'T00:00:00').getTime() : 0;

        let newStatus;
        if (latest && now - latest <= ACTIVE_MS) {
          newStatus = 'active';
        } else if (latest && now - latest <= DORMANT_MS) {
          newStatus = 'dormant';
        } else {
          newStatus = 'archived';
        }

        if (job.status !== newStatus) {
          job.status = newStatus;
          job.updated_at = new Date().toISOString();
          await MiiDB.save('cost_jobs', job);
          counts[newStatus]++;
        } else {
          counts.unchanged++;
        }
      }

      return counts;
    },

    // ── Analysis Queries ───────────────────────────────────────────

    /**
     * Supplier spend breakdown: GROUP BY supplier_name.
     * Includes Purchase, General Cost, and Commitment transaction types.
     * Sorted by total spend descending.
     * @param {string} jobId
     * @returns {Promise<Array>} [{supplier_name, total, count}]
     */
    async getSupplierSpend(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);
      const validTypes = ['Purchase', 'General Cost', 'Commitment'];

      const grouped = txns
        .filter(t => validTypes.includes(t.trans_type))
        .reduce((acc, t) => {
          const key = t.supplier_name || '(Unknown)';
          if (!acc[key]) acc[key] = { supplier_name: key, total: 0, count: 0 };
          acc[key].total += parseFloat(t.total_cost) || 0;
          acc[key].count++;
          return acc;
        }, {});

      return Object.values(grouped).sort((a, b) => Math.abs(b.total) - Math.abs(a.total));
    },

    /**
     * Labour spend breakdown: GROUP BY cost_code.
     * Hours are quantity/100 (Opera 3 stores in hundredths).
     * @param {string} jobId
     * @returns {Promise<Array>} [{cost_code, total, hours, count}]
     */
    async getLabourSpend(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const grouped = txns
        .filter(t => t.trans_type === 'Labour')
        .reduce((acc, t) => {
          const key = t.cost_code || '(Unknown)';
          if (!acc[key]) acc[key] = { cost_code: key, total: 0, hours: 0, count: 0 };
          acc[key].total += parseFloat(t.total_cost) || 0;
          acc[key].hours += (parseFloat(t.quantity) || 0) / 100;
          acc[key].count++;
          return acc;
        }, {});

      return Object.values(grouped).sort((a, b) => Math.abs(b.total) - Math.abs(a.total));
    },

    /**
     * Get all revenue transactions for a job.
     * @param {string} jobId
     * @returns {Promise<Array>}
     */
    async getRevenueTransactions(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);
      return txns.filter(t => t.is_revenue === true);
    },

    /**
     * Weekly spend breakdown: GROUP BY week commencing (Monday) and category.
     * @param {string} jobId
     * @returns {Promise<Array>} [{week_commencing, categories: {1: total, 2: total, ...}, total}]
     */
    async getWeeklySpend(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const weeks = {};
      for (const t of txns) {
        if (t.is_revenue) continue;
        const wc = getWeekCommencing(t.trans_date || '1970-01-01');
        if (!weeks[wc]) {
          weeks[wc] = { week_commencing: wc, categories: {}, total: 0 };
        }
        const cat = t.mapped_category || 0;
        weeks[wc].categories[cat] = (weeks[wc].categories[cat] || 0) + (parseFloat(t.total_cost) || 0);
        weeks[wc].total += parseFloat(t.total_cost) || 0;
      }

      return Object.values(weeks).sort((a, b) => a.week_commencing.localeCompare(b.week_commencing));
    },

    /**
     * Monthly spend breakdown: GROUP BY YYYY-MM.
     * @param {string} jobId
     * @returns {Promise<Array>} [{month, total, count}]
     */
    async getMonthlySpend(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const months = txns
        .filter(t => !t.is_revenue)
        .reduce((acc, t) => {
          const m = getYearMonth(t.trans_date);
          if (!acc[m]) acc[m] = { month: m, total: 0, count: 0 };
          acc[m].total += parseFloat(t.total_cost) || 0;
          acc[m].count++;
          return acc;
        }, {});

      return Object.values(months).sort((a, b) => a.month.localeCompare(b.month));
    },

    /**
     * Top N suppliers by total spend, with per-category breakdown.
     * @param {string} jobId
     * @param {number} limit - max results (default 15)
     * @returns {Promise<Array>} [{supplier_name, total, categories: {1: amt, ...}}]
     */
    async getTopSuppliers(jobId, limit = 15) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const grouped = txns
        .filter(t => !t.is_revenue)
        .reduce((acc, t) => {
          const key = t.supplier_name || '(Unknown)';
          if (!acc[key]) acc[key] = { supplier_name: key, total: 0, categories: {} };
          const cost = parseFloat(t.total_cost) || 0;
          acc[key].total += cost;
          const cat = t.mapped_category || 0;
          acc[key].categories[cat] = (acc[key].categories[cat] || 0) + cost;
          return acc;
        }, {});

      return Object.values(grouped)
        .sort((a, b) => Math.abs(b.total) - Math.abs(a.total))
        .slice(0, limit);
    },

    /**
     * Top N transactions by absolute value.
     * @param {string} jobId
     * @param {number} limit - max results (default 25)
     * @returns {Promise<Array>}
     */
    async getTopTransactions(jobId, limit = 25) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      return txns
        .sort((a, b) => Math.abs(parseFloat(b.total_cost) || 0) - Math.abs(parseFloat(a.total_cost) || 0))
        .slice(0, limit);
    },

    /**
     * Overtime analysis: parse cost_code into trade prefix + numeric suffix.
     * Suffix '01' = normal time, anything else = overtime.
     * @param {string} jobId
     * @returns {Promise<Array>} per-trade breakdown
     */
    async getOvertimeAnalysis(jobId) {
      await MiiDB.ready();
      const txns = await getByField('cost_transactions', 'job_id', jobId);

      const trades = {};
      const codePattern = /^(.*?)(\d{1,2})$/;

      for (const t of txns) {
        if (t.trans_type !== 'Labour') continue;

        const code = (t.cost_code || '').trim();
        const match = code.match(codePattern);
        if (!match) continue;

        const tradePrefix = match[1];
        const suffix = match[2];
        const isOT = suffix !== '01';
        const hours = (parseFloat(t.quantity) || 0) / 100;
        const cost = parseFloat(t.total_cost) || 0;

        if (!trades[tradePrefix]) {
          trades[tradePrefix] = {
            trade: tradePrefix,
            normal_hours: 0,
            normal_cost: 0,
            ot_hours: 0,
            ot_cost: 0,
          };
        }

        if (isOT) {
          trades[tradePrefix].ot_hours += hours;
          trades[tradePrefix].ot_cost += cost;
        } else {
          trades[tradePrefix].normal_hours += hours;
          trades[tradePrefix].normal_cost += cost;
        }
      }

      // Calculate derived fields
      return Object.values(trades).map(t => {
        const normalRate = t.normal_hours > 0 ? t.normal_cost / t.normal_hours : 0;
        const otRate = t.ot_hours > 0 ? t.ot_cost / t.ot_hours : 0;
        const premium = normalRate > 0 ? ((otRate - normalRate) / normalRate) * 100 : 0;
        const totalHours = t.normal_hours + t.ot_hours;
        const otPct = totalHours > 0 ? (t.ot_hours / totalHours) * 100 : 0;

        return {
          ...t,
          normal_rate: normalRate,
          ot_rate: otRate,
          premium,
          ot_pct: otPct,
        };
      }).sort((a, b) => (b.normal_cost + b.ot_cost) - (a.normal_cost + a.ot_cost));
    },

    // ── Full Analysis ──────────────────────────────────────────────

    /**
     * Single call to get all analysis data for a job detail view.
     * Returns a flat structure that the UI can consume directly.
     * For groups, merges data from all child jobs.
     * @param {string} jobId
     * @returns {Promise<Object>}
     */
    async getFullAnalysis(jobId) {
      await MiiDB.ready();

      const job = await this.getJob(jobId);
      if (!job) return null;

      // Determine which job IDs to query for transactions
      let txnJobIds;
      if (job.is_group) {
        txnJobIds = await this.getGroupChildIds(jobId);
      } else {
        txnJobIds = [jobId];
      }

      // Gather all transactions across relevant jobs
      let allTxns = [];
      for (const jid of txnJobIds) {
        const txns = await getByField('cost_transactions', 'job_id', jid);
        allTxns = allTxns.concat(txns);
      }

      // Normalize transaction fields for UI consumption
      const transactions = allTxns.map(t => ({
        ...t,
        date: t.trans_date,
        category_number: t.mapped_category || 0,
        category_name: t.mapped_category ? (DEFAULT_CATEGORIES.find(c => c.number === t.mapped_category) || {}).name || '' : '',
        supplier: t.supplier_name || '',
        employee: t.surname ? (t.surname + (t.forename ? ', ' + t.forename : '')) : '',
        qty: t.trans_type === 'Labour' ? ((parseFloat(t.quantity) || 0) / 100).toFixed(1) : (t.quantity || ''),
        hours: t.trans_type === 'Labour' ? ((parseFloat(t.quantity) || 0) / 100).toFixed(1) : null,
        value: parseFloat(t.value) || 0,
        overhead: parseFloat(t.overhead_value) || 0,
        total_cost: parseFloat(t.total_cost) || 0,
        source: t.mapping_source || '',
      })).sort((a, b) => (a.date || '').localeCompare(b.date || ''));

      // Compute enriched analysis
      const overtimeAnalysis = await this._computeOvertimeFromTxns(allTxns);
      const monthlySpend = this._computeMonthlyFromTxns(allTxns);
      const topSuppliers = this._computeTopSuppliersFromTxns(allTxns, 15);
      const weeklySpend = this._computeWeeklyFromTxns(allTxns);

      // Totals
      const totals = {
        contract_value: job.total_contract || 0,
        budget_at_cost: job.total_at_cost || 0,
        actual_spend: job.total_actual || 0,
        committed: job.total_committed || 0,
        total_exposure: job.total_exposure || 0,
        total_revenue: job.total_revenue || 0,
      };

      return {
        job,
        categories: job.categories || [],
        totals,
        transactions,
        overtime_analysis: overtimeAnalysis,
        monthly_spend: monthlySpend,
        top_suppliers: topSuppliers,
        weekly_spend: weeklySpend,
      };
    },

    /**
     * Compute overtime analysis from raw transactions.
     * @private
     */
    async _computeOvertimeFromTxns(txns) {
      const trades = {};
      const codePattern = /^(.*?)(\d{1,2})$/;

      for (const t of txns) {
        if (t.trans_type !== 'Labour') continue;
        const code = (t.cost_code || '').trim();
        const match = code.match(codePattern);
        if (!match) continue;

        const tradePrefix = match[1];
        const suffix = match[2];
        const isOT = suffix !== '01';
        const hours = (parseFloat(t.quantity) || 0) / 100;
        const cost = parseFloat(t.total_cost) || 0;

        if (!trades[tradePrefix]) {
          trades[tradePrefix] = { trade: tradePrefix, normal_hours: 0, normal_cost: 0, ot_hours: 0, ot_cost: 0 };
        }

        if (isOT) {
          trades[tradePrefix].ot_hours += hours;
          trades[tradePrefix].ot_cost += cost;
        } else {
          trades[tradePrefix].normal_hours += hours;
          trades[tradePrefix].normal_cost += cost;
        }
      }

      return Object.values(trades).map(t => {
        const normalRate = t.normal_hours > 0 ? t.normal_cost / t.normal_hours : 0;
        const otRate = t.ot_hours > 0 ? t.ot_cost / t.ot_hours : 0;
        const premium = normalRate > 0 ? ((otRate - normalRate) / normalRate) * 100 : 0;
        const totalHours = t.normal_hours + t.ot_hours;
        const otPct = totalHours > 0 ? (t.ot_hours / totalHours) * 100 : 0;
        return { ...t, normal_rate: normalRate, ot_rate: otRate, rate_premium: premium, ot_percentage: otPct };
      }).sort((a, b) => (b.normal_cost + b.ot_cost) - (a.normal_cost + a.ot_cost));
    },

    /**
     * Compute monthly spend from raw transactions.
     * @private
     */
    _computeMonthlyFromTxns(txns) {
      const months = {};
      for (const t of txns) {
        if (t.is_revenue) continue;
        const m = getYearMonth(t.trans_date);
        if (!m || m.length < 7) continue;
        if (!months[m]) months[m] = { month: m, spend: 0, count: 0, cumulative: 0 };
        months[m].spend += parseFloat(t.total_cost) || 0;
        months[m].count++;
      }
      const sorted = Object.values(months).sort((a, b) => a.month.localeCompare(b.month));
      let cum = 0;
      for (const m of sorted) {
        cum += m.spend;
        m.cumulative = cum;
      }
      return sorted;
    },

    /**
     * Compute top suppliers from raw transactions.
     * @private
     */
    _computeTopSuppliersFromTxns(txns, limit) {
      const grouped = {};
      for (const t of txns) {
        if (t.is_revenue) continue;
        const key = t.supplier_name || '(Unknown)';
        if (!grouped[key]) grouped[key] = { supplier: key, total_spend: 0, categories: {}, count: 0 };
        const cost = parseFloat(t.total_cost) || 0;
        grouped[key].total_spend += cost;
        grouped[key].count++;
        const cat = t.mapped_category || 0;
        grouped[key].categories[cat] = (grouped[key].categories[cat] || 0) + cost;
      }
      return Object.values(grouped)
        .map(s => {
          // Find primary category (highest spend)
          let maxCat = 0, maxVal = 0;
          for (const [c, v] of Object.entries(s.categories)) {
            if (Math.abs(v) > Math.abs(maxVal)) { maxCat = parseInt(c); maxVal = v; }
          }
          s.primary_category = maxCat ? (DEFAULT_CATEGORIES.find(d => d.number === maxCat) || {}).name || '' : '';
          return s;
        })
        .sort((a, b) => Math.abs(b.total_spend) - Math.abs(a.total_spend))
        .slice(0, limit);
    },

    /**
     * Compute weekly spend from raw transactions.
     * @private
     */
    _computeWeeklyFromTxns(txns) {
      const weeks = {};
      for (const t of txns) {
        if (t.is_revenue) continue;
        const wc = getWeekCommencing(t.trans_date || '1970-01-01');
        if (!weeks[wc]) weeks[wc] = { week_commencing: wc, categories: {}, total: 0, count: 0 };
        const cat = t.mapped_category || 0;
        weeks[wc].categories[cat] = (weeks[wc].categories[cat] || 0) + (parseFloat(t.total_cost) || 0);
        weeks[wc].total += parseFloat(t.total_cost) || 0;
        weeks[wc].count++;
      }
      return Object.values(weeks).sort((a, b) => a.week_commencing.localeCompare(b.week_commencing));
    },

    // ── Import Pipeline ────────────────────────────────────────────

    /**
     * Process an Opera 3 Excel workbook import.
     * Parses the Excel file, groups rows by job_number, and imports each.
     *
     * @param {Object} workbook - XLSX workbook object
     * @param {string} filename - original filename
     * @returns {Promise<Object>} {message, jobs_updated, total_inserted, total_deleted}
     */
    async processImport(workbook, filename) {
      await MiiDB.ready();

      // Parse the first sheet into JSON rows
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const rawRows = XLSX.utils.sheet_to_json(sheet, { raw: false });

      if (!rawRows || rawRows.length === 0) {
        throw new Error('No data found in file');
      }

      // Normalize column names: Opera 3 exports have varying headers
      const rows = rawRows.map(row => {
        const r = {};
        for (const [key, val] of Object.entries(row)) {
          r[key.trim().toLowerCase().replace(/[\s\/]+/g, '_')] = val;
        }
        return r;
      });

      // Extract job numbers from data
      const jobGroups = {};
      for (const row of rows) {
        const jobNum = (row.job_number || row.job || row.job_no || '').toString().trim();
        if (!jobNum) continue;
        if (!jobGroups[jobNum]) jobGroups[jobNum] = [];

        // Parse date - handle DD/MM/YYYY or YYYY-MM-DD
        let transDate = row.date || row.trans_date || row.transaction_date || '';
        if (transDate && transDate.includes('/')) {
          const parts = transDate.split('/');
          if (parts.length === 3) {
            transDate = parts[2] + '-' + parts[1].padStart(2, '0') + '-' + parts[0].padStart(2, '0');
          }
        }

        const txn = {
          trans_date: transDate,
          cost_code: (row.cost_code || row.costcode || '').toString().trim(),
          cost_code_desc: (row.cost_code_desc || row.costcode_desc || row.cost_code_description || '').toString().trim(),
          supplier_name: (row.supplier || row.supplier_name || '').toString().trim(),
          trans_type: (row.trans_type || row.type || row.transaction_type || '').toString().trim(),
          cost_type: (row.cost_type || '').toString().trim(),
          cost_type_desc: (row.cost_type_desc || row.cost_type_description || '').toString().trim(),
          quantity: parseFloat(row.quantity || row.qty || row.hours || 0) || 0,
          value: parseFloat(row.value || row.amount || 0) || 0,
          total_cost: parseFloat(row.total_cost || row.total || row.net_value || row.value || 0) || 0,
          overhead_value: parseFloat(row.overhead || row.overhead_value || 0) || 0,
          description: (row.description || row.desc || '').toString().trim(),
          jw_trtype: (row.jw_trtype || row.trtype || '').toString().trim(),
          subcontractor: (row.subcontractor || '').toString().trim(),
          surname: (row.surname || row.employee || '').toString().trim(),
          forename: (row.forename || '').toString().trim(),
          po_number: (row.po_number || row.po || row.order_no || '').toString().trim(),
        };

        jobGroups[jobNum].push(txn);
      }

      const jobNumbers = Object.keys(jobGroups);
      if (jobNumbers.length === 0) {
        throw new Error('No job numbers found in data');
      }

      let totalInserted = 0, totalDeleted = 0;
      const jobsUpdated = [];

      for (const jobNumber of jobNumbers) {
        const result = await this._processJobImport(jobGroups[jobNumber], jobNumber, filename);
        totalInserted += result.inserted;
        totalDeleted += result.deleted;
        jobsUpdated.push(jobNumber);
      }

      return {
        message: `Imported ${totalInserted} transactions across ${jobsUpdated.length} job(s)`,
        count: totalInserted,
        jobs_updated: jobsUpdated,
        total_inserted: totalInserted,
        total_deleted: totalDeleted,
      };
    },

    /**
     * Internal: process import for a single job number.
     * @private
     */
    async _processJobImport(fileData, jobNumber, filename) {
      // Find or create job
      const allJobs = await MiiDB.getAll('cost_jobs');
      let job = allJobs.find(j => j.job_number === jobNumber);
      let jobId;

      if (job) {
        jobId = job.id;
      } else {
        jobId = await this.createJob({
          job_number: jobNumber,
          job_name: 'Job ' + jobNumber,
          markup: 14,
          categories: DEFAULT_CATEGORIES.map(c => ({ number: c.number, contract_value: 0 })),
        });
      }

      // Extract date range
      const dates = fileData.map(t => t.trans_date).filter(Boolean).sort();
      const startDate = dates[0] || '';
      const endDate = dates[dates.length - 1] || '';

      // Date-range replace
      let deleted = 0;
      if (startDate && endDate) {
        deleted = await this.deleteTransactionsInDateRange(jobId, startDate, endDate);
      }

      // Deduplicate
      const seen = new Set();
      const unique = [];
      let duplicatesSkipped = 0;

      for (const t of fileData) {
        const key = [
          (t.trans_date || ''),
          (t.cost_code || '').toUpperCase(),
          (t.supplier_name || '').toUpperCase(),
          (t.surname || '').toUpperCase(),
          String(parseFloat(t.value) || 0),
          String(parseFloat(t.total_cost) || 0),
          (t.po_number || '').toUpperCase(),
          (t.description || '').toUpperCase(),
        ].join('|');

        if (seen.has(key)) {
          duplicatesSkipped++;
        } else {
          seen.add(key);
          unique.push(t);
        }
      }

      // Create import batch
      const importBatchId = generateId();
      await MiiDB.save('cost_imports', {
        id: importBatchId,
        job_id: jobId,
        job_number: jobNumber,
        filename: filename || '',
        file_date_start: startDate,
        file_date_end: endDate,
        total_rows: fileData.length,
        unique_rows: unique.length,
        duplicates_skipped: duplicatesSkipped,
        deleted_before_insert: deleted,
        imported_at: new Date().toISOString(),
      });

      // Insert
      const inserted = await this.importTransactions(jobId, unique, importBatchId);

      // Apply mappings
      await this.applyMappings(jobId);

      // Update last_import_at
      const jobRecord = await MiiDB.get('cost_jobs', jobId);
      if (jobRecord) {
        jobRecord.last_import_at = new Date().toISOString();
        jobRecord.updated_at = new Date().toISOString();
        await MiiDB.save('cost_jobs', jobRecord);
      }

      // Fire-and-forget server sync after import
      this.fullSyncJob(jobId).catch(() => {});

      return { job_id: jobId, inserted, deleted, duplicates_skipped: duplicatesSkipped };
    },

    // ── Server Sync (fire-and-forget) ─────────────────────────────

    /**
     * Sync a job (with categories) to the server.
     * Call after createJob, updateJob, updateMarkup, saveBudgets, etc.
     */
    async syncJobToServer(jobId) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        const job = await MiiDB.get('cost_jobs', jobId);
        if (!job) return;
        const cats = await MiiDB.getAll('cost_categories');
        job.categories = cats.filter(c => c.job_id === jobId);
        const resp = await fetch(MII_API + '/cost/jobs', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify(job),
        });
        if (resp.ok) console.log('[CostSync] Job synced:', job.job_number);
        else console.warn('[CostSync] Job sync error:', resp.status);
      } catch (e) {
        console.log('[CostSync] Offline:', e.message);
      }
    },

    /**
     * Sync transactions for a job to the server.
     * Optionally pass a date replace_range {start, end}.
     */
    async syncTransactionsToServer(jobId, replaceRange) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        const allTxns = await MiiDB.getAll('cost_transactions');
        const jobTxns = allTxns.filter(t => t.job_id === jobId);
        if (!jobTxns.length) return;

        // Send in batches of 500
        const BATCH = 500;
        for (let i = 0; i < jobTxns.length; i += BATCH) {
          const batch = jobTxns.slice(i, i + BATCH);
          const payload = { job_id: jobId, transactions: batch };
          if (i === 0 && replaceRange) payload.replace_range = replaceRange;
          await fetch(MII_API + '/cost/transactions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
            body: JSON.stringify(payload),
          });
        }
        console.log('[CostSync] Txns synced:', jobTxns.length, 'for job', jobId);
      } catch (e) {
        console.log('[CostSync] Txn sync offline:', e.message);
      }
    },

    /**
     * Sync mapping rule updates to server.
     */
    async syncMappingToServer(mapping, type) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        await fetch(MII_API + '/cost/mappings', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify({ ...mapping, type }),
        });
        console.log('[CostSync] Mapping synced:', type, mapping.id);
      } catch (e) {
        console.log('[CostSync] Mapping sync offline:', e.message);
      }
    },

    /**
     * Sync an import record to the server.
     */
    async syncImportToServer(importRecord) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        await fetch(MII_API + '/cost/imports', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify(importRecord),
        });
        console.log('[CostSync] Import synced:', importRecord.id);
      } catch (e) {
        console.log('[CostSync] Import sync offline:', e.message);
      }
    },

    /**
     * Ask AI to suggest categories for unmapped transactions.
     * Returns array of {txn_index, category_number, confidence, reason}.
     */
    async aiSuggestCategories(jobId) {
      const token = localStorage.getItem('mii_token');
      if (!token) throw new Error('Not logged in');

      const allTxns = await MiiDB.getAll('cost_transactions');
      const unmapped = allTxns.filter(t => t.job_id === jobId && (!t.mapped_category || t.mapped_category === 0));
      if (!unmapped.length) return [];

      // Get existing mappings for context
      const supplierMappings = await MiiDB.getAll('cost_supplier_mappings');
      const labourMappings = await MiiDB.getAll('cost_labour_mappings');

      const resp = await fetch(MII_API + '/cost/ai-suggest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
        body: JSON.stringify({
          transactions: unmapped.slice(0, 50).map(t => ({
            supplier_name: t.supplier_name || t.subcontractor || '',
            cost_code: t.cost_code || '',
            cost_code_desc: t.cost_code_desc || '',
            trans_type: t.trans_type || '',
            description: t.description || '',
            value: t.value || t.total_cost || 0,
          })),
          existing_mappings: {
            supplier: supplierMappings.slice(0, 30),
            labour: labourMappings.slice(0, 30),
          },
        }),
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || 'AI suggestion failed');
      }

      const data = await resp.json();
      return { suggestions: data.suggestions || [], unmapped_txns: unmapped.slice(0, 50) };
    },

    /**
     * Full sync — push all cost data for a job to the server.
     * Called after imports or major changes.
     */
    async fullSyncJob(jobId) {
      await this.syncJobToServer(jobId);
      await this.syncTransactionsToServer(jobId);
    },

    // ── Exposed Helpers ────────────────────────────────────────────

    generateId,
    formatGBP,
    formatGBPSigned,
    pctOf,
    getWeekCommencing,
    getYearMonth,
    DEFAULT_CATEGORIES,
  };

  const MII_API = 'https://mii-hub-api.azurewebsites.net/api';

  // Expose globally
  global.CostDB = CostDB;

})(typeof self !== 'undefined' ? self : this);
