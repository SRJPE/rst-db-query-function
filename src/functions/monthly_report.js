const { app } = require('@azure/functions')
const pg = require('pg')

app.http('monthly_report', {
  methods: ['GET', 'POST'],
  authLevel: 'anonymous',
  handler: async (request, context) => {
    // Define variables to store connection details and credentials
    const config = {
      host: process.env['POSTGRES_HOST'],
      user: process.env['POSTGRES_USER'],
      password: process.env['POSTGRES_PASSWORD'],
      database: process.env['POSTGRES_DB'],
      port: 5432,
      ssl: true,
    }

    // Create query to execute against the database
    const catchQuery = `
      SELECT 
      tv.trap_visit_time_start, 
      tv.trap_visit_time_end, 
      tl.trap_name as trap_name,
      t.commonname as species_common_name,
      r.definition as capture_run,
      mt.definition as mark_type,
      cr.adipose_clipped, 
      cr.dead,
      ls.definition as life_stage,
      cr.fork_length,
      cr.weight,
      cr.num_fish_caught, 
      cr.plus_count,
      pcm.definition as plus_count_methodology,
      mt.definition as mark_type,
      mc.definition as mark_color,
      bp.definition as mark_position
      FROM catch_raw cr 
      left join trap_visit tv on (cr.trap_visit_id = tv.id)
      left join trap_locations tl on (tv.trap_location_id = tl.id) 
      left join program p on (cr.program_id = p.id)
      left join run r on (cr.capture_run_class = r.id)
      left join taxon t on (cr.taxon_code = t.code)
      left join life_stage ls on (cr.life_stage = ls.id)
      left join plus_count_methodology pcm on (cr.plus_count_methodology = pcm.id)
      left join existing_marks em on (cr.id = em.catch_raw_id)
      left join mark_type mt on (em.mark_type_id = mt.id)
      left join mark_color mc on (em.mark_color_id  = mc.id)
      left join body_part bp on (em.mark_position_id  = bp.id)
      where (tv.program_id = 1 or tv.program_id = 2) and tv.trap_visit_time_start >= CURRENT_DATE - INTERVAL '1 month';
    `

    const trapQuery = `
      SELECT 
      p.program_name as program_name, 
      tl.trap_name as trap_name,
      t.is_paper_entry, 
      t.trap_visit_time_start, 
      t.trap_visit_time_end,
      fp.definition as fish_processed,
      wfnp.definition as why_fish_not_processed,
      t.cone_depth,
      tf.definition as trap_functioning,
      tsae.definition as trap_status_at_end, 
      t.total_revolutions, 
      t.rpm_at_start, 
      t.rpm_at_end, 
      t.debris_volume_gal 
      FROM trap_visit t
      left join program p on (t.program_id = p.id)
      left join fish_processed fp on (t.fish_processed = fp.id)
      left join why_fish_not_processed wfnp on (t.why_fish_not_processed = wfnp.id)
      left join trap_functionality tf on (t.trap_functioning = tf.id)
      left join trap_status_at_end tsae on (t.trap_status_at_end = tsae.id)
      left join trap_locations tl on (t.trap_location_id = tl.id) 
      where (t.program_id = 1 or t.program_id = 2) and t.trap_visit_time_start >= CURRENT_DATE - INTERVAL '1 month';
    `

    const catchQuerySpec = {
      text: catchQuery,
    }
    const trapQuerySpec = {
      text: trapQuery,
    }

    try {
      // Create a pool of connections
      const pool = new pg.Pool(config)

      // Get a new client connection from the pool
      const client = await pool.connect()

      // Execute the query against the client
      const catchResult = await client.query(catchQuerySpec)
      const trapResult = await client.query(trapQuerySpec)

      // Release the connection
      client.release()

      console.log(catchResult.rows)

      // calculating for all fish caught
      const length = catchResult.rows.length
      let totalWeight = 0
      let weightCounter = 0
      let totalForkLength = 0
      let forkLengthCounter = 0
      let numFishCaught = 0
      const speciesTracker = {}

      catchResult.rows.forEach(row => {
        numFishCaught += row.num_fish_caught
        speciesTracker[row.species_common_name] = speciesTracker[
          row.species_common_name
        ]
          ? (speciesTracker[row.species_common_name] = {
              ...speciesTracker[row.species_common_name],
              totalFishCount:
                speciesTracker[row.species_common_name].totalFishCount +
                row.num_fish_caught,
            })
          : (speciesTracker[row.species_common_name] = {
              forkLength: 0,
              forkLengthCount: 0,
              weight: 0,
              weightCount: 0,
              totalFishCount: row.num_fish_caught,
            })

        if (row.weight) {
          const rowWeight = parseFloat(row.weight)
          totalWeight += rowWeight
          weightCounter++
          speciesTracker[row.species_common_name] = speciesTracker[
            row.species_common_name
          ]
            ? (speciesTracker[row.species_common_name] = {
                ...speciesTracker[row.species_common_name],
                weight:
                  speciesTracker[row.species_common_name].weight + rowWeight,
                weightCount:
                  speciesTracker[row.species_common_name].weightCount + 1,
              })
            : (speciesTracker[row.species_common_name] = {
                weight: rowWeight,
                weightCount: 1,
                forkLength: 0,
                forkLengthCount: 0,
              })
        }
        if (row.fork_length) {
          const rowForkLength = parseFloat(row.fork_length)
          totalForkLength += rowForkLength
          forkLengthCounter++
          speciesTracker[row.species_common_name] = speciesTracker[
            row.species_common_name
          ]
            ? (speciesTracker[row.species_common_name] = {
                ...speciesTracker[row.species_common_name],
                forkLength:
                  speciesTracker[row.species_common_name].forkLength +
                  rowForkLength,
                forkLengthCount:
                  speciesTracker[row.species_common_name].forkLengthCount + 1,
              })
            : (speciesTracker[row.species_common_name] = {
                forkLength: rowForkLength,
                forkLengthCount: 1,
                weight: 0,
                weightCount: 0,
              })
        }
      })

      const avgWeight = (totalWeight / weightCounter).toFixed(2)
      const avgForkLength = (totalForkLength / forkLengthCounter).toFixed(2)

      try {
        const summaryValuesRows = [
          {
            Species: 'All',
            'Total Fish Caught': numFishCaught,
            'Records with Fork Length': forkLengthCounter,
            'Average Fork Length': avgForkLength,
            'Records with Weight': weightCounter,
            'Average Weight': avgWeight,
          },
          ...Object.keys(speciesTracker)
            .map(key => {
              return {
                Species: key,
                'Total Fish Caught': speciesTracker[key].totalFishCount,
                'Records with Fork Length': speciesTracker[key].forkLengthCount,
                'Average Fork Length': speciesTracker[key].forkLength
                  ? (
                      speciesTracker[key].forkLength /
                      speciesTracker[key].forkLengthCount
                    ).toFixed(2)
                  : 'NA',
                'Records with Weight': speciesTracker[key].weightCount,
                'Average Weight': speciesTracker[key].weight
                  ? (
                      speciesTracker[key].weight /
                      speciesTracker[key].weightCount
                    ).toFixed(2)
                  : 'NA',
              }
            })
            .sort((a, b) => b['Total Fish Caught'] - a['Total Fish Caught']),
        ]
        const body = {
          catchResults: JSON.stringify(catchResult.rows),
          trapResults: JSON.stringify(trapResult.rows),
          catchSummaryResults: JSON.stringify(summaryValuesRows),
        }

        return { body: JSON.stringify(body) }

        // Return the query resuls back to the caller as JSON
      } catch (error) {
        console.log('error', error)
      }
    } catch (err) {
      context.log(err.message)
      return { body: 'Error' }
    }
  },
})
