
  
    
        create or replace table `upi_dev`.`silver`.`silver_transactions`
      
      using delta
      
      
      
      
      
      
      
      as
      select
    trim(txn_id) as txn_id,
    timestamp as event_timestamp,
    Lower(trim(sender_upi)) as sender_upi,
    Lower(trim(sender_state)) as sender_state,
    trim(sender_device_id) as sender_device_id,
    Lower(trim(receiver_upi)) as receiver_upi,
    trim(receiver_type) as receiver_type,
    trim(receiver_category) as receiver_category,
    amount,
    trim(status) as status
from `upi_dev`.`bronze`.`upi_raw`
  