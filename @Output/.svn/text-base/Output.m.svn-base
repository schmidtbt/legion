classdef Output < handle
    
    properties
        data;
    end
    
    methods
        
        function obj = Output( data )
            obj.data = data;
        end
        
    end
    
    methods (Static)
        
        function arr = extractor( output, scaleIdx, winIdx )
            
            arr = zeros( length(output), 1 );
            for i = 1:length(output)
                arr(i) = output{i}{1}{scaleIdx}{winIdx};
                
            end
            
        end
    end
    
end