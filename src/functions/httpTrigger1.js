const { app } = require('@azure/functions')
const pg = require('pg')

app.http('httpTrigger1', {
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
      cr.release_id,
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
      where (tv.program_id = 1 or tv.program_id = 2) and tv.trap_visit_time_start >= CURRENT_DATE - INTERVAL '24 hours';
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
      t.debris_volume_liters 
      FROM trap_visit t
      left join program p on (t.program_id = p.id)
      left join fish_processed fp on (t.fish_processed = fp.id)
      left join why_fish_not_processed wfnp on (t.why_fish_not_processed = wfnp.id)
      left join trap_functionality tf on (t.trap_functioning = tf.id)
      left join trap_status_at_end tsae on (t.trap_status_at_end = tsae.id)
      left join trap_locations tl on (t.trap_location_id = tl.id) 
      where (t.program_id = 1 or t.program_id = 2) and t.trap_visit_time_start >= CURRENT_DATE - INTERVAL '24 hours';
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

      // Return the query resuls back to the caller as JSON
      const body = {
        catchResults: JSON.stringify(catchResult.rows),
        trapResults: JSON.stringify(trapResult.rows),
      }

      return { body: JSON.stringify(body) }
    } catch (err) {
      context.log(err.message)
      return { body: 'Error' }
    }
  },
})
