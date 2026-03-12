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

  // NAECI standard labour grades
  const DEFAULT_GRADES = [
    { grade_number: 1, grade_name: 'Eng. Co-ordinator' },
    { grade_number: 2, grade_name: 'Supervisor' },
    { grade_number: 3, grade_name: 'Grade 6 - PICWP/Chargehand' },
    { grade_number: 4, grade_name: 'Grade 5 - Coded Welder' },
    { grade_number: 5, grade_name: 'Grade 5 - Fitters/Riggers/Welders' },
    { grade_number: 6, grade_name: 'Grade 4 - Craftsman' },
    { grade_number: 7, grade_name: 'Grade 3 - Semi Skilled' },
  ];

  // Rate types (8 NAECI overtime categories)
  const RATE_COLUMNS = [
    { key: 'rate_a', header: 'Rate A (Normal)',   tooltip: 'Normal time: 8h Mon-Thu, 6h Fri' },
    { key: 'rate_b', header: 'Rate B (MW OT)',    tooltip: 'Midweek overtime: after shift to midnight' },
    { key: 'rate_c', header: 'Rate C (Night)',     tooltip: 'Night overtime: midnight to 07:00' },
    { key: 'rate_d', header: 'Rate D (Wknd)',      tooltip: 'Weekend: first 4h Saturday morning' },
    { key: 'rate_e', header: 'Rate E (Wknd+)',     tooltip: 'Weekend: after 4h Saturday to Monday 07:00' },
    { key: 'rate_f', header: 'Rate F',             tooltip: 'Additional rate type F' },
    { key: 'rate_g', header: 'Rate G',             tooltip: 'Additional rate type G' },
    { key: 'rate_h', header: 'Rate H',             tooltip: 'Additional rate type H' },
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
                       estimate_ref = '', ai_notes = '', group_id = null,
                       expected_start = null, expected_end = null, rate_card_id = null }) {
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
        expected_start: expected_start || null,
        expected_end: expected_end || null,
        rate_card_id: rate_card_id || null,
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
        const costCodeDescUpper = (t.cost_code_desc || t.cost_code || '').toUpperCase();

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

    // jw_trtype numeric code → readable trans_type (Opera 3 standard)
    TRTYPE_MAP: { 1: 'Sales Invoice', 2: 'Purchase', 3: 'Labour', 4: 'General Cost', 5: 'Revenue', 6: 'Commitment' },

    // Raw jwipr column index → field mapping (SELECT * FROM jwipr, 52 cols)
    RAW_COLUMN_MAP: {
      0: 'job_number',      // jw_cstdoc
      3: 'cost_code',       // jw_ccode
      7: 'cost_type_desc',  // jw_pcode
      9: 'cost_type',       // jw_csttype
      10: 'description',    // jw_desc
      11: 'jw_trtype',      // jw_trtype (numeric)
      12: 'trans_date',     // jw_trdate
      15: 'quantity',       // jw_qty
      17: 'value',          // jw_value
      18: 'overhead_value', // jw_ohead
      23: 'surname',        // jw_wgemp (employee code)
      25: 'supplier_name',  // jw_placc (supplier code)
      28: 'subcontractor',  // jw_subcnt
      32: 'po_number',      // jw_podoc
    },

    // Curated format header aliases (case-insensitive matching)
    HEADER_ALIASES: {
      job_number: ['JOB NUMBER', 'JOB', 'JOB NO', 'JOBNUMBER', 'JOB_NUMBER'],
      cost_code: ['COST CODE', 'COSTCODE', 'CODE'],
      cost_code_desc: ['COST CODE DESC', 'COST CODE DESCRIPTION', 'CODE DESC', 'COSTCODEDESC'],
      supplier_name: ['SUPPLIER', 'SUPPLIER NAME', 'SUPPLIERNAME'],
      cost_type: ['COST TYPE', 'COSTTYPE', 'TYPE'],
      cost_type_desc: ['COST TYPE DESC', 'COST TYPE DESCRIPTION', 'COSTTYPEDESC', 'TYPE DESC'],
      quantity: ['QUANTITY', 'QTY'],
      value: ['VALUE', 'AMOUNT'],
      description: ['DESCRIPTION', 'DESC', 'NARRATIVE'],
      jw_trtype: ['JW_TRTYPE', 'TRTYPE', 'JW TRTYPE'],
      trans_type: ['TRANS TYPE', 'TRANSTYPE', 'TRANSACTION TYPE'],
      overhead_value: ['OVERHEAD VALUE', 'OVERHEAD', 'OVERHEADVALUE'],
      subcontractor: ['SUBCONTRACTOR', 'SUB CONTRACTOR', 'SUB'],
      trans_date: ['DATE', 'TRANS DATE', 'TRANSDATE', 'TRANSACTION DATE'],
      surname: ['SURNAME', 'LAST NAME'],
      forename: ['FORENAME', 'FIRST NAME', 'FIRSTNAME'],
      total_cost: ['TOTAL COST', 'TOTALCOST', 'TOTAL'],
      po_number: ['PO NUMBER', 'PONUMBER', 'PO', 'PO NO'],
    },

    /**
     * Detect if a header row represents raw jwipr format (52+ cols, starts with jw_cstdoc).
     */
    _isRawFormat(header) {
      if (!header || header.length < 50) return false;
      const first = (header[0] || '').toString().trim().toLowerCase();
      return first === 'jw_cstdoc' || header.some(h => (h || '').toString().trim().toLowerCase() === 'jw_cstdoc');
    },

    /**
     * Build header→index map for curated format using header aliases.
     */
    _buildHeaderMap(header) {
      const upper = header.map(h => (h || '').toString().trim().toUpperCase());
      const map = {};
      for (const [field, aliases] of Object.entries(this.HEADER_ALIASES)) {
        for (const alias of aliases) {
          const idx = upper.indexOf(alias);
          if (idx >= 0) { map[field] = idx; break; }
        }
      }
      return map;
    },

    /**
     * Parse a date value robustly. Handles Excel date objects, DD/MM/YYYY,
     * YYYY-MM-DD, DD.MM.YYYY, and corrupt dates (year > 2030).
     * Always returns YYYY-MM-DD string or ''.
     */
    _parseDate(raw, rowData) {
      if (raw == null || raw === '') return this._parseTrrefDate(rowData);

      // Excel serial number (SheetJS sometimes gives these)
      if (typeof raw === 'number') {
        const d = new Date((raw - 25569) * 86400000);
        if (!isNaN(d.getTime()) && d.getFullYear() <= 2030) {
          return d.toISOString().split('T')[0];
        }
        return this._parseTrrefDate(rowData);
      }

      const s = raw.toString().trim();
      if (!s) return this._parseTrrefDate(rowData);

      // Try YYYY-MM-DD
      if (/^\d{4}-\d{2}-\d{2}/.test(s)) {
        const year = parseInt(s.substring(0, 4));
        if (year > 2030) return this._parseTrrefDate(rowData);
        return s.substring(0, 10);
      }

      // Try DD/MM/YYYY or DD.MM.YYYY
      const match = s.match(/^(\d{1,2})[\/.](\d{1,2})[\/.](\d{4})/);
      if (match) {
        const year = parseInt(match[3]);
        if (year > 2030) return this._parseTrrefDate(rowData);
        return match[3] + '-' + match[2].padStart(2, '0') + '-' + match[1].padStart(2, '0');
      }

      // Try Date constructor as fallback
      const d = new Date(s);
      if (!isNaN(d.getTime()) && d.getFullYear() <= 2030) {
        return d.toISOString().split('T')[0];
      }

      return this._parseTrrefDate(rowData);
    },

    /**
     * Fallback: parse date from jw_trref (column 13) which often has dd.mm.yyyy.
     * Only applicable for raw jwipr format.
     */
    _parseTrrefDate(rowData) {
      if (!rowData || !Array.isArray(rowData) || rowData.length <= 13) return '';
      const trref = (rowData[13] || '').toString().trim();
      if (!trref) return '';

      // dd.mm.yyyy or dd/mm/yyyy
      const match = trref.match(/^(\d{1,2})[\/.](\d{1,2})[\/.](\d{4})/);
      if (match) {
        return match[3] + '-' + match[2].padStart(2, '0') + '-' + match[1].padStart(2, '0');
      }

      // yyyy-mm-dd
      if (/^\d{4}-\d{2}-\d{2}/.test(trref)) {
        return trref.substring(0, 10);
      }

      return '';
    },

    /**
     * Parse a raw jwipr row (52 columns) into standard transaction dict.
     */
    _parseRawRow(rowData) {
      if (!rowData || rowData.length < 33) return null;

      const result = {};

      // Map fixed columns
      for (const [colStr, field] of Object.entries(this.RAW_COLUMN_MAP)) {
        const col = parseInt(colStr);
        result[field] = col < rowData.length ? rowData[col] : null;
      }

      // Job number required
      const jobNum = (result.job_number || '').toString().trim();
      if (!jobNum) return null;
      result.job_number = jobNum;

      // Derive trans_type from numeric jw_trtype
      const rawTrtype = result.jw_trtype;
      if (rawTrtype != null) {
        const trInt = parseInt(rawTrtype);
        if (!isNaN(trInt)) {
          result.trans_type = this.TRTYPE_MAP[trInt] || ('Type ' + trInt);
          result.jw_trtype = String(trInt);
        } else {
          result.trans_type = (rawTrtype || '').toString().trim();
          result.jw_trtype = (rawTrtype || '').toString().trim();
        }
      } else {
        result.trans_type = '';
        result.jw_trtype = '';
      }

      // Parse date with corrupt-date handling (year > 2030 → fallback to jw_trref)
      result.trans_date = this._parseDate(result.trans_date, rowData);

      // Numeric fields → clean floats (2 decimal)
      const val = parseFloat(result.value) || 0;
      const ovh = parseFloat(result.overhead_value) || 0;
      result.value = Math.round(val * 100) / 100;
      result.overhead_value = Math.round(ovh * 100) / 100;
      result.quantity = Math.round((parseFloat(result.quantity) || 0) * 100) / 100;

      // Compute total_cost = value + overhead (desktop parity)
      result.total_cost = Math.round((val + ovh) * 100) / 100;

      // String fields → trim
      for (const f of ['cost_code', 'cost_type_desc', 'description', 'subcontractor',
                        'supplier_name', 'surname', 'po_number', 'cost_type']) {
        result[f] = (result[f] || '').toString().trim();
      }

      // Fields not in raw format
      result.cost_code_desc = '';
      result.forename = '';

      // Commitment detection: jw_trtype 6 = Commitment, PO/CO codes
      const trUpper = result.jw_trtype.toUpperCase();
      result.is_commitment = (trUpper === '6' || trUpper === 'PO' || trUpper === 'CO');

      // Revenue detection: cost_code Z99
      result.is_revenue = (result.cost_code.toUpperCase() === 'Z99');

      return result;
    },

    /**
     * Parse a curated-format row using header map.
     */
    _parseCuratedRow(rowData, headerMap) {
      if (!rowData) return null;

      const result = {};
      for (const [field, idx] of Object.entries(headerMap)) {
        result[field] = idx < rowData.length ? rowData[idx] : null;
      }

      // Job number required
      const jobNum = (result.job_number || '').toString().trim();
      if (!jobNum) return null;
      result.job_number = jobNum;

      // Parse date
      result.trans_date = this._parseDate(result.trans_date, null);

      // Derive trans_type from jw_trtype if trans_type not present
      const rawTrtype = (result.jw_trtype || '').toString().trim();
      if (rawTrtype && !result.trans_type) {
        const trInt = parseInt(rawTrtype);
        if (!isNaN(trInt)) {
          result.trans_type = this.TRTYPE_MAP[trInt] || ('Type ' + trInt);
          result.jw_trtype = String(trInt);
        }
      }

      // Numeric fields
      result.value = Math.round((parseFloat(result.value) || 0) * 100) / 100;
      result.overhead_value = Math.round((parseFloat(result.overhead_value) || 0) * 100) / 100;
      result.quantity = Math.round((parseFloat(result.quantity) || 0) * 100) / 100;

      // Compute total_cost = value + overhead (desktop parity)
      // Only use file's total_cost if no overhead (backwards compat)
      const fileTotalCost = parseFloat(result.total_cost) || 0;
      if (result.overhead_value !== 0) {
        result.total_cost = Math.round((result.value + result.overhead_value) * 100) / 100;
      } else if (fileTotalCost !== 0) {
        result.total_cost = Math.round(fileTotalCost * 100) / 100;
      } else {
        result.total_cost = result.value;
      }

      // String fields → trim
      for (const f of ['cost_code', 'cost_code_desc', 'supplier_name', 'cost_type',
                        'cost_type_desc', 'description', 'subcontractor', 'surname',
                        'forename', 'po_number', 'trans_type']) {
        result[f] = (result[f] || '').toString().trim();
      }

      // Commitment detection
      const trUpper = (result.jw_trtype || '').toString().trim().toUpperCase();
      result.is_commitment = (trUpper === '6' || trUpper === 'PO' || trUpper === 'CO');

      // Revenue detection
      result.is_revenue = ((result.cost_code || '').toUpperCase() === 'Z99');

      return result;
    },

    /**
     * Compute SHA-256-like hash for deduplication (all 17 fields + mapping fields).
     * Uses a deterministic string concat since browser crypto is async.
     */
    _computeRowHash(row) {
      const parts = [
        row.trans_date || '',
        (row.job_number || '').toUpperCase(),
        (row.cost_code || '').toUpperCase(),
        (row.cost_code_desc || '').toUpperCase(),
        (row.supplier_name || '').toUpperCase(),
        (row.cost_type || '').toUpperCase(),
        (row.cost_type_desc || '').toUpperCase(),
        String(row.quantity || 0),
        String(row.value || 0),
        String(row.total_cost || 0),
        String(row.overhead_value || 0),
        (row.description || '').toUpperCase(),
        (row.jw_trtype || '').toUpperCase(),
        (row.trans_type || '').toUpperCase(),
        (row.subcontractor || '').toUpperCase(),
        (row.surname || '').toUpperCase(),
        (row.forename || '').toUpperCase(),
        (row.po_number || '').toUpperCase(),
      ];
      return parts.join('\x00');
    },

    /**
     * Process an Opera 3 Excel/CSV workbook import.
     * Auto-detects raw jwipr (52 cols) vs curated (18 cols with headers).
     * Supports both XLSX (SheetJS workbook) and CSV (SheetJS workbook from CSV).
     *
     * @param {Object} workbook - XLSX workbook object (SheetJS)
     * @param {string} filename - original filename
     * @param {Object} [opts] - options
     * @param {boolean} [opts.skipCommitments] - skip jw_trtype=6 rows (stale POs)
     * @param {string}  [opts.dateAfter] - only import rows after this date (YYYY-MM-DD)
     * @param {Array}   [opts.jobFilter] - only import these job numbers
     * @returns {Promise<Object>} {message, jobs_updated, total_inserted, total_deleted, format}
     */
    async processImport(workbook, filename, opts = {}) {
      await MiiDB.ready();

      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];

      // Get raw array-of-arrays (not JSON) so we can detect format
      const aoa = XLSX.utils.sheet_to_json(sheet, { header: 1, raw: true });

      if (!aoa || aoa.length < 2) {
        throw new Error('No data found in file');
      }

      const header = aoa[0].map(h => (h || '').toString().trim());
      const isRaw = this._isRawFormat(header);
      const format = isRaw ? 'raw_jwipr' : 'curated';
      console.log('[Import] Detected format:', format, '(' + header.length + ' columns)');

      // Parse all data rows
      const parsedRows = [];
      const jobFilter = opts.jobFilter ? new Set(opts.jobFilter.map(j => j.toString().trim())) : null;

      if (isRaw) {
        for (let i = 1; i < aoa.length; i++) {
          const rowData = aoa[i];
          // Early skip: check job_number (col 0) before full parse
          if (jobFilter) {
            const rawJob = (rowData[0] || '').toString().trim();
            if (!jobFilter.has(rawJob)) continue;
          }
          const parsed = this._parseRawRow(rowData);
          if (parsed) parsedRows.push(parsed);
        }
      } else {
        const headerMap = this._buildHeaderMap(header);
        if (!headerMap.job_number && headerMap.job_number !== 0) {
          throw new Error('No Job Number column found. Headers: ' + header.join(', '));
        }
        const jobColIdx = headerMap.job_number;
        for (let i = 1; i < aoa.length; i++) {
          const rowData = aoa[i];
          // Early skip
          if (jobFilter) {
            const rawJob = (rowData[jobColIdx] || '').toString().trim();
            if (!jobFilter.has(rawJob)) continue;
          }
          const parsed = this._parseCuratedRow(rowData, headerMap);
          if (parsed) parsedRows.push(parsed);
        }
      }

      // Filter: skip commitments (historical files with stale POs)
      let filteredRows = parsedRows;
      if (opts.skipCommitments) {
        const before = filteredRows.length;
        filteredRows = filteredRows.filter(r => (r.jw_trtype || '').toString().trim() !== '6');
        const skipped = before - filteredRows.length;
        if (skipped) console.log('[Import] Skipped', skipped, 'commitment rows');
      }

      // Filter: date cutoff (avoid overlap between files)
      if (opts.dateAfter) {
        const cutoff = opts.dateAfter;
        const before = filteredRows.length;
        filteredRows = filteredRows.filter(r => {
          if (!r.trans_date) return true; // keep rows without dates
          return r.trans_date > cutoff;
        });
        const skipped = before - filteredRows.length;
        if (skipped) console.log('[Import] Skipped', skipped, 'rows before', cutoff);
      }

      if (filteredRows.length === 0) {
        return { message: 'No valid data rows after filtering', count: 0, jobs_updated: [], format };
      }

      // Group by job number
      const jobGroups = {};
      for (const row of filteredRows) {
        const jn = row.job_number;
        if (!jobGroups[jn]) jobGroups[jn] = [];
        jobGroups[jn].push(row);
      }

      const jobNumbers = Object.keys(jobGroups);
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
        format,
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

      // Deduplicate using full-field hash (desktop parity: SHA-256 of all 17+5 fields)
      const seen = new Set();
      const unique = [];
      let duplicatesSkipped = 0;

      for (const t of fileData) {
        const key = this._computeRowHash(t);
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
     * Fetch transactions for a job from the server (SharePoint → API → client).
     * Server-side equivalent of desktop's _process_staging_for_job().
     * Downloads both All_Batches.csv and MII_Daily_Export.xlsx from SharePoint,
     * parses, filters by job number, deduplicates, and returns transactions.
     *
     * @param {string} jobId - existing job ID in IndexedDB
     * @param {string} jobNumber - job number to filter by
     * @returns {Promise<Object>} {imported, total_from_server, already_existed}
     */
    async processJobFromServer(jobId, jobNumber) {
      const token = localStorage.getItem('mii_token');
      if (!token) throw new Error('Not logged in');

      // Call server endpoint
      const resp = await fetch(MII_API + '/cost/process-job', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
        body: JSON.stringify({ job_number: jobNumber }),
      });

      if (resp.status === 503) {
        throw new Error('SharePoint integration not configured on server');
      }
      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || 'Server import failed');
      }

      const data = await resp.json();
      const serverTxns = data.transactions || [];

      if (serverTxns.length === 0) {
        return { imported: 0, total_from_server: 0, already_existed: 0 };
      }

      // Compute date range for date-range replace
      const dates = serverTxns.map(t => t.trans_date).filter(Boolean).sort();
      const startDate = dates[0] || '';
      const endDate = dates[dates.length - 1] || '';

      // Delete existing transactions in this date range (date-range replace)
      let deleted = 0;
      if (startDate && endDate) {
        deleted = await this.deleteTransactionsInDateRange(jobId, startDate, endDate);
      }

      // Deduplicate within the batch
      const seen = new Set();
      const unique = [];
      for (const t of serverTxns) {
        const key = this._computeRowHash(t);
        if (!seen.has(key)) {
          seen.add(key);
          unique.push(t);
        }
      }

      // Create import batch record
      const importBatchId = generateId();
      await MiiDB.save('cost_imports', {
        id: importBatchId,
        job_id: jobId,
        job_number: jobNumber,
        filename: 'SharePoint (All_Batches + Daily Export)',
        file_date_start: startDate,
        file_date_end: endDate,
        total_rows: serverTxns.length,
        unique_rows: unique.length,
        duplicates_skipped: serverTxns.length - unique.length,
        deleted_before_insert: deleted,
        imported_at: new Date().toISOString(),
        source: 'server',
      });

      // Insert transactions
      const inserted = await this.importTransactions(jobId, unique, importBatchId);

      // Update last_import_at
      const jobRecord = await MiiDB.get('cost_jobs', jobId);
      if (jobRecord) {
        jobRecord.last_import_at = new Date().toISOString();
        await MiiDB.save('cost_jobs', jobRecord);
      }

      return {
        imported: inserted,
        total_from_server: serverTxns.length,
        already_existed: deleted,
        all_batches_count: data.all_batches_count || 0,
        daily_export_count: data.daily_export_count || 0,
      };
    },

    /**
     * Ask AI to suggest reusable mapping RULES for unmapped transactions.
     * Ported from desktop ai_mapping_service.py — sends deduplicated patterns,
     * returns supplier/labour mapping rules (not per-transaction suggestions).
     *
     * @param {string} jobId
     * @returns {Promise<Object>} {supplier_mappings, labour_mappings, unmapped_count, pattern_count}
     */
    async aiSuggestMappings(jobId) {
      const token = localStorage.getItem('mii_token');
      if (!token) throw new Error('Not logged in');

      const allTxns = await MiiDB.getAll('cost_transactions');
      const unmapped = allTxns.filter(t =>
        t.job_id === jobId &&
        (!t.mapped_category || t.mapped_category === 0) &&
        !t.is_revenue &&
        t.mapping_source !== 'manual'
      );
      if (!unmapped.length) return { supplier_mappings: [], labour_mappings: [], unmapped_count: 0, pattern_count: 0 };

      // Deduplicate into patterns grouped by (supplier_name, cost_code) — desktop parity
      const seen = {};
      for (const t of unmapped) {
        const supplier = (t.supplier_name || '').trim();
        const costCode = (t.cost_code || '').trim();
        const key = supplier + '\x00' + costCode;
        if (!seen[key]) {
          seen[key] = {
            supplier_name: supplier,
            cost_code: costCode,
            cost_code_desc: (t.cost_code_desc || '').trim(),
            cost_type: (t.cost_type || '').trim(),
            cost_type_desc: (t.cost_type_desc || '').trim(),
            description: (t.description || '').trim(),
            jw_trtype: (t.jw_trtype || '').trim(),
            count: 1,
          };
        } else {
          seen[key].count++;
        }
      }

      // Sort by frequency descending, limit to 100 patterns
      const patterns = Object.values(seen)
        .sort((a, b) => b.count - a.count)
        .slice(0, 100);

      // Get existing mappings for context
      const supplierMappings = await MiiDB.getAll('cost_supplier_mappings');
      const labourMappings = await MiiDB.getAll('cost_labour_mappings');

      const resp = await fetch(MII_API + '/cost/ai-suggest', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
        body: JSON.stringify({
          patterns,
          existing_mappings: {
            supplier: supplierMappings.slice(0, 30).map(m => ({
              supplier_prefix: m.supplier_prefix || '',
              cost_code_prefix: m.cost_code_prefix || '',
              category_number: m.category_number,
              note: m.note || '',
            })),
            labour: labourMappings.slice(0, 30).map(m => ({
              cost_code: m.cost_code || '',
              category_number: m.category_number,
              note: m.note || '',
            })),
          },
        }),
      });

      if (!resp.ok) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(err.error || 'AI suggestion failed');
      }

      const data = await resp.json();
      return {
        supplier_mappings: data.supplier_mappings || [],
        labour_mappings: data.labour_mappings || [],
        unmapped_count: unmapped.length,
        pattern_count: patterns.length,
      };
    },

    /**
     * Apply AI-suggested mapping rules: create actual supplier/labour mapping
     * records, then re-run applyMappings to categorise all transactions.
     *
     * @param {string} jobId
     * @param {Array} supplierRules - [{supplier_prefix, cost_code_prefix, category_number, note}]
     * @param {Array} labourRules - [{cost_code, category_number, note}]
     * @returns {Promise<Object>} {supplier_created, labour_created, mapping_result}
     */
    async applyAiMappingRules(jobId, supplierRules, labourRules) {
      const counts = { supplier_created: 0, labour_created: 0 };

      for (const r of supplierRules) {
        await this.createSupplierMapping({
          supplier_prefix: (r.supplier_prefix || '').toUpperCase(),
          cost_code_prefix: (r.cost_code_prefix || '').toUpperCase(),
          category_number: r.category_number,
          job_id: jobId,
          note: r.note || 'AI suggested',
        });
        counts.supplier_created++;
      }

      for (const r of labourRules) {
        await this.createLabourMapping({
          cost_code: (r.cost_code || '').toUpperCase(),
          category_number: r.category_number,
          job_id: jobId,
          note: r.note || 'AI suggested',
        });
        counts.labour_created++;
      }

      // Re-run mapping engine with new rules
      const mappingResult = await this.applyMappings(jobId);
      return { ...counts, mapping_result: mappingResult };
    },

    /**
     * Full sync — push all cost data for a job to the server.
     * Called after imports or major changes.
     */
    async fullSyncJob(jobId) {
      await this.syncJobToServer(jobId);
      await this.syncTransactionsToServer(jobId);
    },

    // ── Estimates ───────────────────────────────────────────────────

    /**
     * Calculate line item total.
     * Labour: headcount * duration * SUM(hours_X * rate_X)
     * Material: quantity * unit_rate * (1 + markup_pct/100)
     */
    calculateItemTotal(item) {
      if (item.line_type === 'labour') {
        let sum = 0;
        for (const k of ['a','b','c','d','e']) {
          sum += (parseFloat(item['hours_' + k]) || 0) * (parseFloat(item['rate_' + k]) || 0);
        }
        return (parseFloat(item.headcount) || 0) * (parseFloat(item.duration_weeks) || 1) * sum;
      }
      // material
      const qty = parseFloat(item.quantity) || 0;
      const rate = parseFloat(item.unit_rate) || 0;
      const mkp = parseFloat(item.markup_pct) || 0;
      return qty * rate * (1 + mkp / 100);
    },

    /**
     * Calculate man-hours for a labour line item.
     */
    calculateManHours(item) {
      if (item.line_type !== 'labour') return 0;
      let totalHrs = 0;
      for (const k of ['a','b','c','d','e']) {
        totalHrs += parseFloat(item['hours_' + k]) || 0;
      }
      return (parseFloat(item.headcount) || 0) * (parseFloat(item.duration_weeks) || 1) * totalHrs;
    },

    /**
     * Create a new estimate.
     * @returns {Promise<string>} estimate id
     */
    async createEstimate({ name = '', estimate_ref = '', client = '', rate_card_id = null,
                            markup_pct = 14, materials_markup_pct = 2.5, job_id = null, notes = '',
                            start_date = null, pre_stop_weeks = 3, total_weeks = 24,
                            shift_pattern = '7x12h_day', contingency = null }) {
      await MiiDB.ready();
      const id = generateId();
      const now = new Date().toISOString();

      const est = {
        id,
        name,
        estimate_ref,
        client,
        rate_card_id,
        job_id,
        markup_pct: parseFloat(markup_pct) || 14,
        materials_markup_pct: parseFloat(materials_markup_pct) || 2.5,
        status: 'draft',
        start_date: start_date || null,
        pre_stop_weeks: parseInt(pre_stop_weeks) || 3,
        total_weeks: parseInt(total_weeks) || 24,
        shift_pattern: shift_pattern || '7x12h_day',
        contingency: contingency || {},
        notes,
        version: 1,
        created_at: now,
        updated_at: now,
      };
      await MiiDB.save('cost_estimates', est);
      this.syncEstimateToServer(id).catch(() => {});
      return id;
    },

    /**
     * Get all estimates (headers only).
     */
    async getEstimates() {
      await MiiDB.ready();
      const ests = await MiiDB.getAll('cost_estimates');
      return ests.sort((a, b) => (b.updated_at || '').localeCompare(a.updated_at || ''));
    },

    /**
     * Get full estimate with items grouped by section.
     */
    async getEstimate(estimateId) {
      await MiiDB.ready();
      const est = await MiiDB.get('cost_estimates', estimateId);
      if (!est) return null;

      const allItems = await MiiDB.getAll('cost_estimate_items');
      const items = allItems.filter(i => i.estimate_id === estimateId);
      items.sort((a, b) => (a.sort_order || 0) - (b.sort_order || 0));

      // Group by section
      est.sections = {};
      for (let s = 1; s <= 8; s++) {
        est.sections[s] = items.filter(i => i.section_number === s);
      }
      est.items = items;
      return est;
    },

    /**
     * Update estimate header fields.
     */
    async updateEstimate(estimateId, updates) {
      await MiiDB.ready();
      const est = await MiiDB.get('cost_estimates', estimateId);
      if (!est) return;
      Object.assign(est, updates, { updated_at: new Date().toISOString() });
      await MiiDB.save('cost_estimates', est);
      this.syncEstimateToServer(estimateId).catch(() => {});
    },

    /**
     * Delete estimate and all its items.
     */
    async deleteEstimate(estimateId) {
      await MiiDB.ready();
      const allItems = await MiiDB.getAll('cost_estimate_items');
      for (const item of allItems) {
        if (item.estimate_id === estimateId) await MiiDB.remove('cost_estimate_items', item.id);
      }
      await MiiDB.remove('cost_estimates', estimateId);
    },

    /**
     * Add a line item to an estimate section.
     * @param {string} estimateId
     * @param {number} sectionNumber - 1-9
     * @param {string} lineType - 'labour', 'material', 'task', or 'auto'
     * @param {Object} data - line item fields
     * @returns {Promise<string>} item id
     */
    async addEstimateItem(estimateId, sectionNumber, lineType, data = {}) {
      await MiiDB.ready();

      // Get next sort order
      const allItems = await MiiDB.getAll('cost_estimate_items');
      const sectionItems = allItems.filter(i => i.estimate_id === estimateId && i.section_number === sectionNumber);
      const maxOrder = sectionItems.reduce((m, i) => Math.max(m, i.sort_order || 0), 0);

      const item = {
        id: generateId(),
        estimate_id: estimateId,
        section_number: sectionNumber,
        sort_order: maxOrder + 1,
        line_type: lineType,
        description: data.description || '',
        // CBS schedule fields
        rate: data.rate || 0,
        qty_per_week: data.qty_per_week || 0,
        schedule: data.schedule || [],
        num_weeks: 0,
        cost_per_week: 0,
        task_id: data.task_id || null,
        auto_source: data.auto_source || null,
        // Labour/rate card fields
        grade_id: data.grade_id || null,
        grade_name: data.grade_name || '',
        headcount: data.headcount || 0,
        duration_weeks: data.duration_weeks || 1,
        hours_a: data.hours_a || 0, hours_b: data.hours_b || 0,
        hours_c: data.hours_c || 0, hours_d: data.hours_d || 0, hours_e: data.hours_e || 0,
        rate_a: data.rate_a || 0, rate_b: data.rate_b || 0,
        rate_c: data.rate_c || 0, rate_d: data.rate_d || 0, rate_e: data.rate_e || 0,
        // Material fields
        quantity: data.quantity || 0,
        unit: data.unit || 'each',
        unit_rate: data.unit_rate || 0,
        markup_pct: data.markup_pct || 0,
        // Task fields (for line_type='task')
        grade_1_count: data.grade_1_count || 0,
        grade_2_count: data.grade_2_count || 0,
        grade_3_count: data.grade_3_count || 0,
        grade_4_count: data.grade_4_count || 0,
        grade_5_count: data.grade_5_count || 0,
        grade_6_count: data.grade_6_count || 0,
        grade_7_count: data.grade_7_count || 0,
        // Computed
        line_total: 0,
        notes: data.notes || '',
      };
      // CBS total: rate × qty_per_week × active_weeks
      item.num_weeks = (item.schedule || []).reduce((s, v) => s + (v ? 1 : 0), 0);
      item.cost_per_week = (item.rate || 0) * (item.qty_per_week || 0);
      item.line_total = item.cost_per_week * item.num_weeks;
      await MiiDB.save('cost_estimate_items', item);

      // Touch estimate updated_at
      const est = await MiiDB.get('cost_estimates', estimateId);
      if (est) { est.updated_at = new Date().toISOString(); await MiiDB.save('cost_estimates', est); }

      return item.id;
    },

    /**
     * Update a line item and recalculate its total.
     */
    async updateEstimateItem(itemId, updates) {
      await MiiDB.ready();
      const item = await MiiDB.get('cost_estimate_items', itemId);
      if (!item) return;
      Object.assign(item, updates);
      item.line_total = this.calculateItemTotal(item);
      await MiiDB.save('cost_estimate_items', item);
    },

    /**
     * Remove a line item.
     */
    async removeEstimateItem(itemId) {
      await MiiDB.ready();
      await MiiDB.remove('cost_estimate_items', itemId);
    },

    /**
     * Get estimate summary: totals per section + grand total.
     */
    async getEstimateSummary(estimateId) {
      const est = await this.getEstimate(estimateId);
      if (!est) return null;

      const markup = 1 + (est.markup_pct || 14) / 100;
      const sections = [];
      let grandAtCost = 0;
      let totalManHours = 0;

      for (let s = 1; s <= 8; s++) {
        const items = est.sections[s] || [];
        const atCost = items.reduce((sum, i) => sum + (i.line_total || 0), 0);
        const manHours = items.reduce((sum, i) => sum + this.calculateManHours(i), 0);
        grandAtCost += atCost;
        totalManHours += manHours;
        sections.push({
          section_number: s,
          name: DEFAULT_CATEGORIES[s - 1].name,
          item_count: items.length,
          at_cost: atCost,
          contract_value: atCost * markup,
          man_hours: manHours,
        });
      }

      return {
        sections,
        grand_at_cost: grandAtCost,
        grand_contract_value: grandAtCost * markup,
        total_man_hours: totalManHours,
        markup_pct: est.markup_pct,
      };
    },

    /**
     * Apply rate card rates to all labour items in an estimate.
     */
    async applyRateCardToEstimate(estimateId, rateCardId) {
      const card = await this.getRateCard(rateCardId);
      if (!card || !card.grades) return 0;

      const allItems = await MiiDB.getAll('cost_estimate_items');
      const labourItems = allItems.filter(i => i.estimate_id === estimateId && i.line_type === 'labour');
      let updated = 0;

      for (const item of labourItems) {
        // Match grade by grade_name or grade_id
        let grade = null;
        if (item.grade_id) grade = card.grades.find(g => g.id === item.grade_id);
        if (!grade && item.grade_name) {
          grade = card.grades.find(g => g.grade_name === item.grade_name);
        }
        if (!grade) continue;

        item.rate_a = parseFloat(grade.rate_a) || 0;
        item.rate_b = parseFloat(grade.rate_b) || 0;
        item.rate_c = parseFloat(grade.rate_c) || 0;
        item.rate_d = parseFloat(grade.rate_d) || 0;
        item.rate_e = parseFloat(grade.rate_e) || 0;
        item.grade_id = grade.id;
        item.line_total = this.calculateItemTotal(item);
        await MiiDB.save('cost_estimate_items', item);
        updated++;
      }

      // Update estimate rate_card_id
      await this.updateEstimate(estimateId, { rate_card_id: rateCardId });
      return updated;
    },

    /**
     * Sync estimate + items to server.
     */
    async syncEstimateToServer(estimateId) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        const est = await this.getEstimate(estimateId);
        if (!est) return;
        const resp = await fetch(MII_API + '/cost/estimates', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify(est),
        });
        if (resp.ok) console.log('[CostSync] Estimate synced:', est.name);
      } catch (e) {
        console.log('[CostSync] Estimate sync offline:', e.message);
      }
    },

    // ── Rate Cards ─────────────────────────────────────────────────

    /**
     * Create a new rate card with default 7 NAECI grades.
     * @param {Object} params - { name, client, year }
     * @returns {Promise<string>} rate card id
     */
    async createRateCard({ name, client = '', year = new Date().getFullYear() }) {
      await MiiDB.ready();
      const cardId = generateId();
      const now = new Date().toISOString();

      const card = {
        id: cardId,
        name: name || `${client || 'New'} ${year}`,
        client,
        year: parseInt(year) || new Date().getFullYear(),
        is_active: true,
        created_at: now,
        updated_at: now,
      };
      await MiiDB.save('cost_rate_cards', card);

      // Create default 7 grades with zero rates
      for (const def of DEFAULT_GRADES) {
        const grade = {
          id: generateId(),
          rate_card_id: cardId,
          grade_number: def.grade_number,
          grade_name: def.grade_name,
          rate_a: '0', rate_b: '0', rate_c: '0', rate_d: '0',
          rate_e: '0', rate_f: '0', rate_g: '0', rate_h: '0',
          sort_order: def.grade_number,
          created_at: now,
        };
        await MiiDB.save('cost_rate_grades', grade);
      }

      this.syncRateCardToServer(cardId).catch(() => {});
      return cardId;
    },

    /**
     * Get all rate cards.
     * @returns {Promise<Array>}
     */
    async getRateCards() {
      await MiiDB.ready();
      const cards = await MiiDB.getAll('cost_rate_cards');
      return cards.sort((a, b) => (b.year || 0) - (a.year || 0));
    },

    /**
     * Get a single rate card with its grades.
     * @param {string} cardId
     * @returns {Promise<Object|null>}
     */
    async getRateCard(cardId) {
      await MiiDB.ready();
      const card = await MiiDB.get('cost_rate_cards', cardId);
      if (!card) return null;

      const allGrades = await MiiDB.getAll('cost_rate_grades');
      card.grades = allGrades
        .filter(g => g.rate_card_id === cardId)
        .sort((a, b) => a.sort_order - b.sort_order);
      return card;
    },

    /**
     * Update rate card metadata.
     * @param {string} cardId
     * @param {Object} updates - { name, client, year, is_active }
     */
    async updateRateCard(cardId, updates) {
      await MiiDB.ready();
      const card = await MiiDB.get('cost_rate_cards', cardId);
      if (!card) return;
      Object.assign(card, updates, { updated_at: new Date().toISOString() });
      await MiiDB.save('cost_rate_cards', card);
      this.syncRateCardToServer(cardId).catch(() => {});
    },

    /**
     * Update a rate grade (rates, name, etc.).
     * @param {string} gradeId
     * @param {Object} updates
     */
    async updateRateGrade(gradeId, updates) {
      await MiiDB.ready();
      const grade = await MiiDB.get('cost_rate_grades', gradeId);
      if (!grade) return;
      Object.assign(grade, updates);
      await MiiDB.save('cost_rate_grades', grade);
      this.syncRateCardToServer(grade.rate_card_id).catch(() => {});
    },

    /**
     * Delete a rate card and all its grades.
     * @param {string} cardId
     */
    async deleteRateCard(cardId) {
      await MiiDB.ready();
      const allGrades = await MiiDB.getAll('cost_rate_grades');
      for (const g of allGrades) {
        if (g.rate_card_id === cardId) await MiiDB.remove('cost_rate_grades', g.id);
      }
      await MiiDB.remove('cost_rate_cards', cardId);
    },

    /**
     * Sync rate card + grades to server.
     */
    async syncRateCardToServer(cardId) {
      const token = localStorage.getItem('mii_token');
      if (!token) return;
      try {
        const card = await this.getRateCard(cardId);
        if (!card) return;
        const resp = await fetch(MII_API + '/cost/rate-cards', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token },
          body: JSON.stringify(card),
        });
        if (resp.ok) console.log('[CostSync] Rate card synced:', card.name);
      } catch (e) {
        console.log('[CostSync] Rate card sync offline:', e.message);
      }
    },

    // ── Exposed Helpers ────────────────────────────────────────────

    generateId,
    formatGBP,
    formatGBPSigned,
    pctOf,
    getWeekCommencing,
    getYearMonth,
    DEFAULT_CATEGORIES,
    DEFAULT_GRADES,
    RATE_COLUMNS,
  };

  const MII_API = 'https://mii-hub-api.azurewebsites.net/api';

  // Expose globally
  global.CostDB = CostDB;

})(typeof self !== 'undefined' ? self : this);
