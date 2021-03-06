package traminer.spark.mapmatching.index;

import java.util.HashSet;

import traminer.util.spatial.objects.Rectangle;

/**
 * A static grid index model made of n x m cells, 
 * with boundary extension.
 * <p>
 * Grid are statically constructed spatial models.
 * <p>
 * The grid is constructed from left to right, 
 * bottom up. The first position in the grid
 * is the position index zero 0=[i,j]=[0,0].
 *  
 * @author uqdalves
 */
@SuppressWarnings("serial")
public class GridModelX implements SpatialIndexModelX {
	/** The grid cells matrix */
	private final Rectangle[][] grid;
	/** The boundaries of this grid diagram */
	private final Rectangle boundary;
	/** Grid dimensions (number of cells in each axis) */
	private final int sizeX;
	private final int sizeY;
	/** Boundary extension threshold, in all cell's directions. */
	private final double extension;
	
	/**
	 * Create a new static grid model of (n x m) cells with 
	 * the given dimensions, plus given boundary extension. 
	 * 
	 * @param n The number of horizontal cells (x).
	 * @param m The number of vertical cells (y).
	 * @param minX Lower-left X coordinate.
	 * @param minY Lower-left Y coordinate.
	 * @param maxX Upper-right X coordinate.
	 * @param maxY Upper-right Y coordinate.
	 * @param ext Boundary extension threshold, in all cell's directions.
	 */	
	public GridModelX(
			int n, int m, double minX, double minY, 
			double maxX, double maxY, double ext) {
		if (n <= 0 || m <= 0) {
			throw new IllegalArgumentException("Grid dimensions "
					+ "must be positive.");
		}
		if (ext < 0) {
			throw new IllegalArgumentException("Boundary extension threshold "
					+ "must not be a negative number.");
		}
		this.grid = new Rectangle[n][m];
		// add boundary threshold to the model boundaries
		this.boundary = new Rectangle(minX-ext, minY-ext, maxX+ext, maxY+ext);
		this.sizeX = n;
		this.sizeY = m;
		this.extension = ext;
		// build the grid with boundary extensions
		build();
		extendBoundary();
	}
	
	/**
	 * Build the model. Generates the grid cells.
	 */
	private void build() {
		// axis increments
		double incrX = (boundary.maxX() - boundary.minX() - 2*extension) / sizeX;
		double incrY = (boundary.maxY() - boundary.minY() - 2*extension) / sizeY;
		double currentX, currentY;
		String cellId;
		currentY = boundary.minY() + extension;
		for (int y=0; y<sizeY; y++) {	
			currentX = boundary.minX() + extension;
			for (int x=0; x<sizeX; x++) {
				cellId = getIndexString(x, y);
				Rectangle cell = new Rectangle(currentX, currentY, 
						currentX+incrX, currentY+incrY);
				cell.setId(cellId);
				grid[x][y] = cell;
				currentX += incrX;
			}
			currentY += incrY;
		}
	}

	/**
	 * Extends the grid cells' boundaries.
	 */
	private void extendBoundary() {
		for (int i = 0; i<sizeX; i++) {
			for (int j = 0; j<sizeY; j++) {
				Rectangle cell = grid[i][j];
				Rectangle extCell = new Rectangle(
						cell.minX()-extension, 
						cell.minY()-extension,
						cell.maxX()+extension, 
						cell.maxY()+extension) ;
				grid[i][j] = extCell;
			}
		}

	}
	
	/**
	 * @return Number of horizontal cells (parallel to the X axis).
	 */
	public int sizeX(){
		return this.sizeX;
	}

	/**
	 * @return Number of vertical cells (parallel to the Y axis).
	 */
	public int sizeY() {
		return this.sizeY;
	}
	
	@Override
	public int size() {
		return (sizeX*sizeY);
	}
	
	@Override
	public boolean isEmpty() {
		if (grid == null) return true;
		return (sizeX == 0 || sizeY == 0);
	}
	
	/**
	 * @return The height (vertical axis) of the cells
	 * in this grid.
	 */
	public double cellsHeight() {
		double height = (boundary.maxY() - boundary.minY()) / sizeY;
		return height;
	}
	
	/**
	 * @return The height (vertical axis) of the cells
	 * in this grid, with the boundaries extensions.
	 */
	public double cellsHeightExt() {
		double height = (boundary.maxY() - boundary.minY()) / sizeY;
		return height + 2*extension;
	}

	/**
	 * @return The width (horizontal axis) of the cells
	 * in this grid.
	 */
	public double cellsWidth() {
		double width = (boundary.maxX() - boundary.minX()) / sizeX;
		return width;
	}
	
	/**
	 * @return The width (horizontal axis) of the cells
	 * in this grid, with the boundaries extensions.
	 */
	public double cellsWidthExt() {
		double width = (boundary.maxX() - boundary.minX()) / sizeX;
		return width + 2*extension;
	}
	
	/**
	 * @return Return the list of grid cells in this model.
	 */
	public Rectangle[][] getCells() {
		return grid;
	}
	
	/**
	 * Return the cell in the position [i,j] of
	 * the grid. Grid i and j position starts from [0,0].
	 * 
 	 * @param i cell position in the horizontal axis (x).
	 * @param j cell position in the vertical axis (y).
	 * 
	 * @return The cell in the position [i,j] of the grid. 
	 */
	public Rectangle get(int i, int j) {
		if (i<0 || i>=sizeX || j<0 && j>=sizeY) {
			throw new IndexOutOfBoundsException(
					"Grid index out of bounds.");
		}	
		return grid[i][j];
	}
	
	@Override
	public Rectangle get(final String index) {
		if (index == null || index.isEmpty()) {
			throw new IllegalArgumentException(
				"Grid index must not be empty.");
		}
		int[] pos = getCellPosition(index);
		return get(pos[0], pos[1]);
	}
	
	@Override
	public Rectangle getBoundary() {
		return boundary;
	}
	
	@Override
	public HashSet<String> search(double x, double y) {
		HashSet<String> posList = new HashSet<>();
		if (!boundary.contains(x, y)) {
			return posList; // didn't find anything
		}		
		
		// get the exact cell containing (x,y)
		int pos[] = searchExact(x, y);
		int i = pos[0];
		int j = pos[1];
		posList.add(getIndexString(i, j));
		
		// if it's in the boundary, check the adjacent cells
		if (!isBoundary(x, y)) {
			return posList;
		}
	
		Rectangle cell;	
		int adjX, adjY;
		
		adjX = i-1; adjY = j-1;
		if (adjX>=0 && adjY>=0) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i; adjY = j-1;
		if (adjY>=0) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i+1; adjY = j-1;
		if (adjX<sizeX && adjY>=0) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i-1; adjY = j;
		if (adjX>=0) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i+1; adjY = j;
		if (adjX<sizeX) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i-1; adjY = j+1;
		if (adjX>=0 && adjY<sizeY) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i; adjY = j+1;
		if (adjY<sizeY) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		adjX = i+1; adjY = j+1;
		if (adjX<sizeX && adjY<sizeY) {
			cell = get(adjX, adjY);
			if (cell.contains(x, y)) {
				posList.add(getIndexString(adjX, adjY));
			}
		}
		
		return posList;
	
	}
	
	@Override
	public boolean isBoundary(double x, double y) {
		if (!boundary.contains(x, y)) {
			return false; // didn't find anything
		}
		// search and get the cell containing the object (x,y)
		int index[] = searchExact(x, y);
		Rectangle cell = get(index[0], index[1]);
		
		// check whether it's within the boundary
		if ((cell.maxX() - x) <= 2*extension) {
			return true;
		}
		if ((x - cell.minX()) <= 2*extension) {
			return true;
		}
		if ((cell.maxY() - y) <= 2*extension) {
			return true;
		}
		if ((y - cell.minY()) <= 2*extension) {
			return true;
		}
		
		return false;
	}
	
	/**
	 * Return the position [i,j] in the grid
	 * of the cell with the given index.
	 * <br> [0] = i cell position in the horizontal axis (x).
	 * <br> [1] = j cell position in the vertical axis (y).

	 * @param index The index of the cell to search.
	 * @return The position [i,j] in the grid
	 * of the cell with the given index.
	 */
	public int[] getCellPosition(final String index){
		try {
			// get cell position
			String pos[] = index.replaceAll("i|j","").split("-");
			int i = Integer.parseInt(pos[0]);
			int j = Integer.parseInt(pos[1]);
			return new int[]{i,j};
		} catch (Exception e) {
			throw new IllegalArgumentException(
					"Grid index is invalid.");
		}	
	}
	
	@Override
	public void print() {
		println("[GRID] [" + sizeX + " x " + sizeY + "]");
		Rectangle cell;
		for(int j=sizeY-1; j>=0; j--){
			for(int i=0; i<sizeX; i++){
				cell = grid[i][j];
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",
						cell.minX(),cell.maxY(),
						cell.maxX(),cell.maxY());
			}	
			println();
			for(int i=0; i<sizeX; i++){
				cell = grid[i][j];
				System.out.format("[(%.2f,%.2f)(%.2f,%.2f)] ",
						cell.minX(),cell.minY(),
						cell.maxX(),cell.minY());
			}
			println(); println();
		}
	}

	/**
	 * The exact cell containing the object in the (x,y) position, 
	 * without considering boundary extensions.
	 * 
	 * @param x
	 * @param y
	 * 
	 * @return The position [i,j] of the cell containing the object (x,y).
	 */
	private int[] searchExact(double x, double y) {	
		double width  = cellsWidth();
		double height = cellsHeight();
		int i = (int) ((x - boundary.minX()) / width);
		int j = (int) ((y - boundary.minY()) / height);
		if (x != boundary.minX() && x % width  == 0) i--;
		if (y != boundary.minY() && y % height == 0) j--;
		
		return new int[]{i, j};
	}

	/**
	 * Generates the cell index.
	 * 
 	 * @param i cell position in the horizontal axis (x).
	 * @param j cell position in the vertical axis (y).
	 * 
	 * @return The index of the cell in the position [i,j]
	 * as a String "i-j".
	 */
	private String getIndexString(final int i, final int j){
		return ("i"+i + "-" +"j"+j);
	}

}
