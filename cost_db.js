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
     * @param {number} params.markup - markup multiplier (e.g. 1.15 for 15%)
     * @param {Array} params.categories - array of {number, contract_value}
     * @param {string} [params.notes]
     * @returns {Promise<string>} job id
     */
    async createJob({ job_number, job_name, client = '', markup = 1.0, categories = [], notes = '' }) {
      await MiiDB.ready();

      const jobId = generateId();
      const now = new Date().toISOString();

      const job = {
        id: jobId,
        job_number,
        job_name,
        client,
        markup: parseFloat(markup) || 1.0,
        notes,
        status: 'active',
        created_at: now,
        updated_at: now,
      };

      await MiiDB.save('cost_jobs', job);

      // Create 8 categories with contract values
      const catLookup = {};
      for (const c of categories) {
        catLookup[c.number] = parseFloat(c.contract_value) || 0;
      }

      for (const def of DEFAULT_CATEGORIES) {
        const contractValue = catLookup[def.number] || 0;
        const atCost = markup > 0 ? contractValue / markup : 0;

        const cat = {
          id: generateId(),
          job_id: jobId,
          category_number: def.number,
          category_name: def.name,
          contract_value: contractValue,
          at_cost: atCost,
          created_at: now,
        };
        await MiiDB.save('cost_categories', cat);
      }

      return jobId;
    },

    /**
     * Get a job with its categories and computed financials.
     * @param {string} jobId
     * @returns {Promise<Object|null>}
     */
    async getJob(jobId) {
      await MiiDB.ready();

      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) return null;

      const categories = await getByField('cost_categories', 'job_id', jobId);
      const financials = await this.getCategoryFinancials(jobId);

      // Merge financials into categories
      const finMap = {};
      for (const f of financials) {
        finMap[f.category_number] = f;
      }

      job.categories = categories
        .sort((a, b) => a.category_number - b.category_number)
        .map(cat => {
          const fin = finMap[cat.category_number] || { actual: 0, committed: 0, exposure: 0, count: 0 };
          return {
            ...cat,
            actual: fin.actual,
            committed: fin.committed,
            exposure: fin.exposure,
            variance: cat.at_cost - fin.exposure,
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

      return job;
    },

    /**
     * Get all jobs for the portfolio dashboard.
     * @returns {Promise<Array>}
     */
    async getAllJobs() {
      await MiiDB.ready();
      const jobs = await MiiDB.getAll('cost_jobs');
      // Enrich each with summary financials
      const enriched = [];
      for (const job of jobs) {
        const full = await this.getJob(job.id);
        if (full) enriched.push(full);
      }
      return enriched;
    },

    /**
     * Update markup for a job and recalculate all category at_cost values.
     * @param {string} jobId
     * @param {number} newMarkup
     */
    async updateMarkup(jobId, newMarkup) {
      await MiiDB.ready();

      const job = await MiiDB.get('cost_jobs', jobId);
      if (!job) throw new Error(`Job ${jobId} not found`);

      const markup = parseFloat(newMarkup) || 1.0;
      job.markup = markup;
      job.updated_at = new Date().toISOString();
      await MiiDB.save('cost_jobs', job);

      // Recalculate at_cost for every category
      const categories = await getByField('cost_categories', 'job_id', jobId);
      for (const cat of categories) {
        cat.at_cost = markup > 0 ? (parseFloat(cat.contract_value) || 0) / markup : 0;
        await MiiDB.save('cost_categories', cat);
      }
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
     * @param {string} jobId
     * @returns {Promise<Object>}
     */
    async getFullAnalysis(jobId) {
      await MiiDB.ready();

      const [
        job,
        supplier_spend,
        labour_spend,
        revenue_transactions,
        weekly_spend,
        monthly_spend,
        top_suppliers,
        top_transactions,
        overtime_analysis,
      ] = await Promise.all([
        this.getJob(jobId),
        this.getSupplierSpend(jobId),
        this.getLabourSpend(jobId),
        this.getRevenueTransactions(jobId),
        this.getWeeklySpend(jobId),
        this.getMonthlySpend(jobId),
        this.getTopSuppliers(jobId),
        this.getTopTransactions(jobId),
        this.getOvertimeAnalysis(jobId),
      ]);

      return {
        job,
        supplier_spend,
        labour_spend,
        revenue_transactions,
        weekly_spend,
        monthly_spend,
        top_suppliers,
        top_transactions,
        overtime_analysis,
      };
    },

    // ── Import Pipeline ────────────────────────────────────────────

    /**
     * Process an Opera 3 data import for a job.
     *
     * 1. Find or auto-create job by job_number
     * 2. Extract date range from transactions
     * 3. Delete existing transactions in that date range
     * 4. Deduplicate within the import batch
     * 5. Create import batch record
     * 6. Bulk insert transactions
     * 7. Apply mappings
     * 8. Return summary
     *
     * @param {Array} fileData - array of parsed Opera 3 transaction objects
     * @param {string} jobNumber - job number to import into
     * @returns {Promise<Object>} {inserted, deleted, duplicates_skipped, mapping_counts}
     */
    async processImport(fileData, jobNumber) {
      await MiiDB.ready();

      if (!fileData || fileData.length === 0) {
        throw new Error('No transaction data to import');
      }

      // 1. Find or create job
      const allJobs = await MiiDB.getAll('cost_jobs');
      let job = allJobs.find(j => j.job_number === jobNumber);
      let jobId;

      if (job) {
        jobId = job.id;
      } else {
        // Auto-create with zero-value categories
        jobId = await this.createJob({
          job_number: jobNumber,
          job_name: jobNumber, // Placeholder, user can rename
          markup: 1.0,
          categories: DEFAULT_CATEGORIES.map(c => ({ number: c.number, contract_value: 0 })),
        });
      }

      // 2. Extract date range from transactions
      const dates = fileData
        .map(t => t.trans_date)
        .filter(Boolean)
        .sort();

      const startDate = dates[0] || '';
      const endDate = dates[dates.length - 1] || '';

      // 3. Delete existing transactions in that date range
      let deleted = 0;
      if (startDate && endDate) {
        deleted = await this.deleteTransactionsInDateRange(jobId, startDate, endDate);
      }

      // 4. Deduplicate within the import batch
      // Key: trans_date + cost_code + supplier + surname + value + total_cost + po_number + description
      const seen = new Set();
      const unique = [];
      let duplicatesSkipped = 0;

      for (const t of fileData) {
        const key = [
          (t.trans_date || '').toUpperCase(),
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

      // 5. Create import batch record
      const importBatchId = generateId();
      const importRecord = {
        id: importBatchId,
        job_id: jobId,
        job_number: jobNumber,
        file_date_start: startDate,
        file_date_end: endDate,
        total_rows: fileData.length,
        unique_rows: unique.length,
        duplicates_skipped: duplicatesSkipped,
        deleted_before_insert: deleted,
        imported_at: new Date().toISOString(),
      };
      await MiiDB.save('cost_imports', importRecord);

      // 6. Bulk insert transactions
      const inserted = await this.importTransactions(jobId, unique, importBatchId);

      // 7. Apply mappings
      const mappingCounts = await this.applyMappings(jobId);

      // 8. Return summary
      return {
        job_id: jobId,
        inserted,
        deleted,
        duplicates_skipped: duplicatesSkipped,
        mapping_counts: mappingCounts,
      };
    },

    // ── Exposed Helpers ────────────────────────────────────────────

    generateId,
    formatGBP,
    formatGBPSigned,
    pctOf,
    DEFAULT_CATEGORIES,
  };

  // Expose globally
  global.CostDB = CostDB;

})(typeof self !== 'undefined' ? self : this);
