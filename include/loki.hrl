
-record(loki_counter, {
          key,
          value
         }).

-record(loki_event, {
          sno,
          event,
          type
         }).

-record(loki, {
          slot_entity,
          data
         }).

