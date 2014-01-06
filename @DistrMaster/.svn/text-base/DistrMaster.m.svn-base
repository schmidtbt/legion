classdef DistrMaster < legion.Master
    
    methods
        
        function obj = DistrMaster( kernel, data, numcores, jobName )
            obj@legion.Master( kernel, data, numcores, jobName );
        end
        
        function exec = executable( obj, idx )
            exec = ['legion.DistrJobrunner( ', num2str( idx ), ', ''', obj.jobPath, ''', ', num2str( obj.realnumcores) ,' );' ];
        end
        
        function stat = status( obj, H )
            
            if( nargin == 1 )
                H = 145;
            end
            
            files = dir( [obj.jobPath, 'output*'] ) ;
            
            stat = zeros( obj.realnumcores );
            
            for i = 1:length(files)
                str = files(i).name;
                ts = find( files(i).name == 't' );
                us = find( files(i).name == '_' );
                first = files(i).name(ts(2)+1:us-1);
                ext =  find( files(i).name == '.' );
                sec = files(i).name( us+1:ext-1 );
                stat( str2num(first), str2num(sec) ) = 1;
                %fprintf( '%s %i %i \n ',files(i).name, str2num(first), str2num(sec) );
            end
            
            numProcs = obj.realnumcores;
            loadableSquares = 0;
            for i = 1:numProcs
                loadableSquares = loadableSquares+i;
            end
            
            complete = length( find( stat == 1 ) );
            percent = complete/ loadableSquares;
            
            figure(H);
            imagesc( stat );
            title(sprintf('Current Job status. Lower-left only. Red blocks indicate completed blocks. Complete: %i / %i ', complete, loadableSquares) );
            
            
        end
        
        % GC Reintegration only, where each output is 2D array of
        % cca_granger_regress outputs
        function [prb, gc, completed] = gcreintegrate( obj, DISPLAY )
            
            % Default Don't Display
            if( nargin == 1 )
                DISPLAY = 0;
            end
            
            
            if( DISPLAY )
                fprintf('Progress will be monitored\n' );
                figure( 143 ); 
            else
                fptinf( 'Progress will not be monitored\n' );
            end
            
            % Allocate two storage variables
            numvertices = size( obj.data, 1 );
            prb = -1.*ones( numvertices,numvertices );
            gc = -1.*ones( numvertices,numvertices );
            completed = zeros( numvertices,numvertices );
            cycle = 1;
            files = dir([obj.jobPath, 'output*']);
            fprintf( 'Found %i output files\n', length(files ) );
            tic;
            for i = 1:length(files)
                fprintf( 'Loading: %s\n', files(i).name );
                loaded = load( files(i).name );
                fprintf( '\t%i of %i\n', i, length(files) );
                
                [rows, cols] = size( loaded.output );
                fprintf( '\tBlock size: %i x %i\n', rows, cols );
                
                % Loop over block output data
                for r = 1:rows
                    for c = 1:cols
                        
                        ridx = loaded.rowidxs(r);
                        cidx = loaded.colidxs(c);
                        
                        TwoOneGC = loaded.output{r,c}.gc(2,1);
                        OneTwoGC = loaded.output{r,c}.gc(1,2);
                        
                        TwoOnePrb = loaded.output{r,c}.prb(2,1);
                        OneTwoPrb = loaded.output{r,c}.prb(1,2);
                        
                        prb( ridx, cidx ) = TwoOnePrb;
                        prb( cidx, ridx ) = OneTwoPrb;
                        
                        gc( ridx, cidx ) = TwoOneGC;
                        gc( cidx, ridx ) = OneTwoGC;
                        
                        % Update the completion matrix to check and ensure
                        % progress
                        completed( ridx, cidx ) = cycle;
                        completed( cidx, ridx ) = cycle;
                        
                    end
                    
                end
                
                
                if( DISPLAY )
                    if( mod( i, 100 ) == 0 || i == 1 )
                        % Update teh cycle value, to display differences
                        % between updates
                        cycle = cycle+1;
                        fprintf( 'Displaying Progress\n' );
                        figure( 143 ); imagesc( completed );
                        elapsedtime = toc;
                        complete = i/length(files);
                        estcompletion = (elapsedtime/complete); % in seconds
                        estremaining = estcompletion - elapsedtime; %in seconds
                        title( sprintf( 'Progress: %i of %i. Elapsed time: %2.3f (seconds), Est Complete in: %2.3f (seconds)', i, length(files), elapsedtime, estremaining ) );
                        drawnow;
                    end
                end
                
            end
            
        end
        
        
    end
    
    methods (Static)
        
        function output = reintegrateOutput( folderpath )
            
            if( nargin == 0 )
                fprintf('Attempting to use current working directory for reintegration\n\n');
                folderpath = pwd;
            else
                fprintf('Attempting to use the following  directory for reintegration: %s\n\n', folderpath );
            end
            
            % Find the output files located in the folderpath
            files = dir('output*');
            output = cell( 0 );
            
            for i = 1:length( files )
                fprintf( 'Loading: %s\n', files(i).name );
                loaded = load( files(i).name );
                fprintf( '\tsize: %i x %i\n', size( loaded.output, 1 ), size( loaded.output, 2 ) );
                
                %disp( loaded.rowidxs );
                %disp( loaded.colidxs );
                
                for row = 1:size( loaded.output, 1 )
                    for col = 1:size( loaded.output, 2 )
                        %loaded.rowidxs(row)
                        %loaded.colidxs(col)
                        output{ loaded.rowidxs(row), loaded.colidxs(col) } = loaded.output{row,col};
                    end
                end
                
            end
            
        end
        
    end
    
end

