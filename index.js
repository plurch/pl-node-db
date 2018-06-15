class DB {
  constructor(knex) {
    this.knex = knex;
  }

  buildInsert(tableName, itemData) {
    const columns = Object.keys(itemData);
    const values = columns.map(c => itemData[c]);
    const valuesPlaceholder = Array(values.length).fill('?').join(',');
    const insertQuery = `INSERT INTO ?? (??) VALUES (${valuesPlaceholder})`;
    return {columns, values, insertQuery};
  }

  upsertItemId({tableName, itemData, condition}) {
    return this.upsertItem({tableName, conflictTarget: 'id', itemData, condition});
  }

  upsertItem({tableName, conflictTarget, itemData, condition = ''}) {
    const {columns, values, insertQuery} = this.buildInsert(tableName, itemData);

    const excludedColumns = columns.filter(c => c !== conflictTarget);
    const excludedPlaceholder = Array(excludedColumns.length).fill('?? = EXCLUDED.??').join(', ');
    const excludedValues = excludedColumns.reduce((acc, c) => acc.concat(c,c), []);
    const query = `${insertQuery} ON CONFLICT (??) DO UPDATE SET ${excludedPlaceholder} ${condition} RETURNING *;`;

    return this.knex
        .raw(query, [tableName, columns].concat(values, conflictTarget, excludedValues))
        .then(result => result.rows[0]);
  }

  upsertItemDoNothing({tableName, itemData}) {
    const {columns, values, insertQuery} = this.buildInsert(tableName, itemData);
    const query = `${insertQuery} ON CONFLICT DO NOTHING RETURNING *;`;

    return this.knex
        .raw(query, [tableName, columns].concat(values))
        .then(result => result.rows[0]);
  }

  async getTime() {
    const start = Date.now();
    await this.knex.raw('SELECT 1;');
    return Date.now() - start;
  }

  async testLatency(count) {
    const times = [];

    for(let i=0; i<count; i++) {
      times.push(await this.getTime());
    }

    console.log('times: ', times);
    console.log('average: ', times.reduce((acc, val) => acc + val, 0) / times.length);
  }
}

module.exports = {
  DB
};
