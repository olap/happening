# Happening backend for paperclip plugin. Copy the file to:
# +config/initializers/+ directory
#

module Paperclip
  module Storage
    module Happening
      def self.extended base
        begin
          require "happening"
        rescue LoadError => e
          e.message << " (You may need to install the happening gem)"
          raise e
        end

        base.instance_eval do
          ::Happening::Log.logger = Rails.logger
          @s3_credentials = parse_credentials(@options.s3_credentials)
          ::Happening::AWS.set_defaults(:bucket => @s3_credentials[:bucket],
                                        :aws_access_key_id => @s3_credentials[:access_key_id],
                                        :aws_secret_access_key => @s3_credentials[:secret_access_key])
        end
      end

      def url(style = default_style)
        ::Happening::S3::Item.new(path(style)).url
      end

      def expiring_url(style = default_style, time = 3600)
        ::Happening::S3::Item.new(path(style)).expiring_url(Time.now + time)
      end

      def exists?(style = default_style)
        ::Happening::S3::Item.new(path(style)).exists?
      end

      def flush_deletes
        attempts = 0
        done = 0
        begin
          EM.run do
            EM.stop if @queued_for_delete.empty?
            @queued_for_delete.each do |path|
              request = ::Happening::S3::Item.new(path).delete
              request.on_success do |response|
                Rails.logger.debug "Deleted! #{path}"
                done += 1
                if done >= @queued_for_delete.count
                  Rails.logger.debug "Finished deleting #{response}"
                  EM.stop
                end
              end
              request.on_error do |error|
                Rails.logger.warn "An error occured: #{error.response_header.status}"
                raise error
              end
            end
          end
        rescue ::StandardError => e
          if attempts < 5
            Rails.logger.debug "failed #{attempts} #{e}"
            attempts += 1
            retry 
          else
            Rails.logger.warn "Giving up failed #{attempts} times"
            raise
          end
        end
      end

      def flush_writes
        attempts = 0
        done = 0
        begin
          EM.run do
            EM.stop if @queued_for_write.empty?
            @queued_for_write.each do |style, file|
              upload = ::Happening::S3::Item.new(path(style)).put(File.read(file))
              upload.on_success do
                Rails.logger.debug "Upload successful! #{file}"
                done += 1
                if done >= @queued_for_write.count
                  EM.stop
                end
              end
              upload.on_error do |error|
                #Rails.logger.warn "Upload failed with: #{error.response_header.status}"
                raise error
              end
            end
          end
        rescue ::StandardError => e
          if attempts < 5
            Rails.logger.debug "fail #{attempts} #{e}"
            attempts += 1
            retry
          else
            Rails.logger.warn "Giving up failed #{attempts} times"
            raise
          end
        end
      end

      def to_file(style = default_style)
        file = nil
        response = nil
        return @queued_for_write[style] if @queued_for_write[style]
        begin
          filename = path(style)
          extname = File.extname(filename)
          basename = File.basename(filename, extname)
          file = Tempfile.new([basename, extname])
          file.binmode if file.respond_to?(:binmode)
          item = nil
          EM.run do
            item = ::Happening::S3::Item.new(filename)
            item.get do |request|
              response = request.response
              Rails.logger.debug "the response content is: #{request.response}"; EM.stop
            end
          end
          file.write(response)
          file.rewind
        rescue ::Happening::Error
          Rails.logger.warn "Happening error"
          file.close if file.respond_to?(:close)
          file = nil
        end
        file
      end

      def parse_credentials creds
        creds = find_credentials(creds).stringify_keys
        env = Object.const_defined?(:Rails) ? Rails.env : nil
        (creds[env] || creds).symbolize_keys
      end

      private
      def find_credentials creds
        case creds
        when File
          YAML::load(ERB.new(File.read(creds.path)).result)
        when String
          YAML::load(ERB.new(File.read(creds)).result)
        when Hash
          creds
        else
          raise ArgumentError, "Credentials are not a path, file, or hash."
        end
      end
    end
  end
end
