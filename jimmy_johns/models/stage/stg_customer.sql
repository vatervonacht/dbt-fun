select
  *
from {{ source('jimmy_johns_source', 'customer') }}
