module Resque
  
  module Plugin
    
    #
    # This hook is really the meaning of our adventure.
    def after_batch_hooks(job)
      job.methods.grep(/^after_batch/).sort
    end
    
  end
  
  module Plugins
    
    module BatchedJob
    
      include Resque::Helpers

      # 
      #   Like enqueue(), but enqueues an entire batch at a time. This ensures
      # that if the first job finishes before any other jobs are enqueued, the
      # first job doesn't think it was the last one and run the finalize hooks.
      def enqueue_batch(batch_id, arglist)
        # Add everything to the batch set first, so we know it won't be empty
        # until we've finished everything in the batch.
        arglist.each { |args| redis.sadd(batch(batch_id), encode(args)) }

        # Enqueue the jobs.
        arglist.each { |args| Resque.enqueue(self, batch_id, *args) }
      end

      #
      # Helper method used to generate the batch key.
      def batch(id)
        "batch:#{id}"
      end

      #
      # Batch the job.  The first argument of a batched job, is the batch id.
      def after_enqueue_batch(id, *args)
        redis.sadd(batch(id), encode(args))
      end
      
=begin
  TODO: Determine if it's necessary to double check the jobs existance 
    before performing it.  If so, do what?
    
      def before_perform_audit_batch(id, *args)
        unless redis.sismember(batch(id), "#{encode(args)}")
          raise Resque::Job::DontPerform.new("#{args} are not a member of #{batch(id)}")
        end
      end
=end

      # 
      #   After every job, no matter in the event of success or failure, we need
      # to remove the job from the batch set.
      def around_perform_amend_batch(id, *args)
        begin
          yield
        ensure
          redis.srem(batch(id), "#{encode(args)}")
        end
      end

      #
      #   After each job is performed, check to see if the job is the last of
      # the given batch.  If so, run after_batch hooks.
      def after_perform_batch(id, *args)
        if batch_complete?(id)
          after_batch_hooks = Resque::Plugin.after_batch_hooks(self)
          after_batch_hooks.each do |hook|
            send(hook, id, *args)
          end
        end
      end
      
      #
      #   Checks to see if the batch key exists.  If the key does exist, is the 
      # set empty?  The Redis srem command deletes the key when the last item 
      # of a set is removed. Ah, go ahead and check the size.
      def batch_complete?(id)
        redis.scard(batch(id)) == 0
      end
      alias :batch_exist? :batch_complete? # => are they?
      
    end
    
  end
  
end
