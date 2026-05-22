"""
Python function to dump CDF file contents similar to cdfdump command.
"""
from pathlib import Path
from spacepy.pycdf import CDF


def cdf_dump(file_path, show_data=True, max_values=10, summary=False, variable=None):
    """
    Display the contents of a CDF file in a human-readable format.
    
    Parameters
    ----------
    file_path : str or Path
        Path to the CDF file to dump.
    show_data : bool, optional
        If True, show sample data values. Default is True.
    max_values : int, optional
        Maximum number of data values to display per variable. Default is 10.
    summary : bool, optional
        If True, only show variable names and record counts. Default is False.
    variable : str, optional
        If provided, only show information for this specific variable. Default is None (show all).
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        print(f"Error: File not found: {file_path}")
        return
    
    print("=" * 80)
    print(f"CDF FILE: {file_path.name}")
    print("=" * 80)
    
    with CDF(str(file_path)) as cdf_file:
        # Filter variables if specific variable requested
        if variable:
            if variable not in cdf_file.keys():
                print(f"Error: Variable '{variable}' not found in CDF file")
                print(f"Available variables: {', '.join(sorted(cdf_file.keys()))}")
                return
            var_list = [variable]
        else:
            var_list = sorted(cdf_file.keys())
        
        # Summary mode - just show variables and record counts
        if summary:
            print("\nVARIABLES (Summary):")
            print("-" * 80)
            for var_name in var_list:
                var = cdf_file[var_name]
                print(f"{var_name:40s} Recs: {len(var)}")
            print("-" * 80)
            print(f"Total variables: {len(var_list)}")
            return
        
        # Global Attributes
        if not variable:  # Only show global attributes when showing all variables
            print("\nGLOBAL ATTRIBUTES:")
            print("-" * 80)
            for attr_name in sorted(cdf_file.attrs.keys()):
                attr_value = cdf_file.attrs[attr_name]
                print(f"  {attr_name:30s} : {attr_value}")
        
        # Variables
        print("\n" + "=" * 80)
        print("VARIABLES:")
        print("=" * 80)
        
        for var_name in var_list:
            var = cdf_file[var_name]
            
            print(f"\n VAR: {var_name}")
            print("-" * 80)
            
            # Variable info
            print(f"  Type: {var.type()}")
            print(f"  Shape: {var.shape}")
            print(f"  Recs: {len(var)}")  # Changed to match cdfdump format
            
            # Variable attributes
            if var.attrs:
                print(f"  Attributes:")
                for attr_name in sorted(var.attrs.keys()):
                    attr_value = var.attrs[attr_name]
                    # Truncate long attribute values
                    if isinstance(attr_value, str) and len(attr_value) > 60:
                        attr_value = attr_value[:57] + "..."
                    print(f"    {attr_name:28s} : {attr_value}")
            
            # Sample data
            if show_data:
                print(f"  Data (first {max_values} values):")
                data = var[...]
                if len(data) > 0:
                    # Show first few values
                    if len(data.shape) == 1:
                        # 1D array
                        sample = data[:max_values]
                        print(f"    {sample}")
                        if len(data) > max_values:
                            print(f"    ... ({len(data) - max_values} more values)")
                    else:
                        # Multi-dimensional
                        sample = data[:min(3, len(data))]
                        for i, row in enumerate(sample):
                            print(f"    [{i}]: {row}")
                        if len(data) > 3:
                            print(f"    ... ({len(data) - 3} more records)")
                else:
                    print("    (empty)")
        
        print("\n" + "=" * 80)
        print(f"Total variables: {len(var_list)}")
        print("=" * 80)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python cdf_dump.py <cdf_file_path> [-v VARIABLE] [--no-data] [--summary]")
        print("  -v VARIABLE : Show only the specified variable")
        print("  --no-data   : Hide data values, show only structure and metadata")
        print("  --summary   : Show only variable names and record counts")
        sys.exit(1)
    
    # Parse arguments
    file_path = None
    variable = None
    show_data = True
    summary = False
    
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg == "-v" and i + 1 < len(sys.argv):
            variable = sys.argv[i + 1]
            i += 2
        elif arg == "--no-data":
            show_data = False
            i += 1
        elif arg == "--summary":
            summary = True
            i += 1
        elif not file_path:
            file_path = arg
            i += 1
        else:
            i += 1
    
    if not file_path:
        print("Error: No CDF file specified")
        sys.exit(1)
    
    cdf_dump(file_path, show_data=show_data, summary=summary, variable=variable)
