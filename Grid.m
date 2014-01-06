classdef Grid < handle
    
    % Implements a distributed matlab system using qsub
    properties (Constant)
        qsubExec = 'qsub  ';
        logLocation = '/synapse/logs/';
        matlabExec = '/synapse/matlab11 -nosplash -nodesktop -nojvm -nodisplay -r ';
    end
    
    properties
        jobname;
        jobPath;
        jobLog ='';
        qsubContents;
        jobExec;
        pbsFile;
    end
    
    methods( Access=public )
        %Class constructor
        %jobName is optional
        function obj = Grid( jobName )
            if( nargin == 1 )
                obj.setJobName( jobName );
            end
        end
        
        %Set Jobname for this run
        function setJobName( obj, jobName )
            obj.jobPath = obj.makeLogDirectory( jobName );
            obj.jobname = jobName;
        end
        
        function setExistingJob( obj, jobName )
            try 
                obj.setJobName( jobName );
            catch
                warning('using existing job. Data could be overwritten');
                obj.jobname = jobName;
                obj.jobPath = obj.getLogDirectory( jobName );
                return;
            end
            error('Job does not exist');
        end
        
        
        function setExecutable( obj, exec, idx )
            
            if( nargin == 2 )
                idx = 1;
            end
            
            exec = [ '\tdisp('''');\n\tdisp(legion.Grid.getDate());\n\t', exec, ';\n\tdisp(legion.Grid.getDate());\n\tquit;\n' ];
            exec = [ '\tcd ', obj.getJobPath(), '\n', exec ];
            %exec = strrep( exec, '"', '\"' );
            obj.jobExec = exec;
            obj.generateQsubFile( idx );
        end
        
        function executePBS( obj, idx )
            
            
            if( nargin == 1 )
                idx_str = '';
            else
                idx_str = ['-',num2str(idx)];
            end
            
            syscmd = obj.qsubExec;
            syscmd = [syscmd, ' -wd ', obj.getJobPath(), ' -N ', obj.getThisUser,'-',obj.getJobName(), idx_str, ' ', obj.getPBSFile() ];
            disp( ['Executing: ', syscmd ]);
            obj.runSysCmd( ['echo "', syscmd, '" >> ', obj.getJobPath(), 'execute.sh'] );
            string = obj.runSysCmd( [syscmd, '| awk ''{print $3}'' '] );
            string = strrep( sprintf(string), sprintf('\n'), '' );
            obj.runSysCmd( ['echo "', string, ' ', obj.getPBSFile(), '" >> ', obj.getJobPath(), 'pids.log'] );
        end
        
        
        function jobname = getJobName(obj)
            if( isempty( obj.jobname ) ); error('Empty JobName. Set the Job name'); end
            jobname = obj.jobname;
        end
        
        function jobpath = getJobPath( obj )
            if( isempty( obj.jobPath ) ); error('Empty JobPath. Set the Job name'); end
            jobpath = obj.jobPath;
        end
        
        function pbsfile = getPBSFile( obj )
            if( isempty( obj.pbsFile ) ); error('Empty pbsFile'); end
            pbsfile = obj.pbsFile;
        end
        
    end
    
    methods( Access=private )
        
        
        
        function populateJobLog( obj )
            
        end
        
        % Creates a .pbs file at the job directory for execution
        function generateQsubFile( obj, idx )
            obj.qsubContents = obj.initQsub();
            obj.qsubContents = [ obj.qsubContents, obj.start_log()];
            obj.qsubContents = [ obj.qsubContents, obj.matlabExec, ' << EOF\n\n',obj.getJobExec,'\nEOF\n\n'];
            obj.qsubContents = [ obj.qsubContents, obj.end_log()];
            obj.qsubContents = [ obj.qsubContents, '\nexit\n\n'];
            file = [obj.jobPath, obj.getJobName(), '-', num2str(idx), '.pbs' ] ;
            disp(['Generating pbs file:', file ] );
            fid = fopen( file, 'w' );
            fprintf( fid,  obj.qsubContents );
            fclose( fid );
            obj.pbsFile = file;
        end
        
        
        function init = initQsub( obj )
            init = '#!/bin/sh\n';
            init = [ init, '\n\necho "Executing @: `date` on host: `hostname` by user: `whoami` "\n']; 
        end
        
        function exec = getJobExec( obj )
            if( isempty( obj.jobExec ) ); error('Empty JobExec. Set the Executable first'); end
            exec = obj.jobExec;
        end
        
        function init_log = start_log( obj )
            init_log = sprintf('logger -i -p local1.info -t legion JOB_ID: ${JOB_ID}: STARTING ${USER} Job: ${JOB_NAME} `date`\n\n');
        end
        
        function end_log = end_log( obj )
            end_log = sprintf('logger -i -p local1.info -t legion JOB_ID: ${JOB_ID}: FINISHED ${USER} Job: ${JOB_NAME}  `date`\n\n');
        end
    end
    
    methods (Static)
        function string = runSysCmd( cmdString )
            [status, string] = system( cmdString );
        end
        
        % Generate a unique log location
        function newPath = makeLogDirectory( jobName )
            newPath = legion.Grid.getLogDirectory( jobName );
            disp(['Creating New Job Directory: ', newPath]);
            [s, mess, id] = mkdir( newPath );
            disp( id );
            if( strcmp(id, 'MATLAB:MKDIR:DirectoryExists' ) )
                error('To prevent data overwritting, you must specify unique jobNames');
            end
        end
        
        function newPath = getLogDirectory( jobName )
            newPath = [legion.Grid.logLocation, legion.Grid.getThisUser(), '/', jobName, '/'];
        end
        
        
        function username = getThisUser()
            [s, username] = system('whoami');
            username = deblank(username);
        end
        
        function hostname = getThisHost()
            [s, hostname] = system('hostname');
            hostname = deblank(hostname);
        end
        
        function date = getDate()
            date = datestr(now, 'mmddyyyy-HH-MM-SS-FFF');
        end
    end
    
    
end