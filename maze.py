from pprint import pprint

DEST_X = 7
DEST_Y = 5


# Read file into a list. A file consists of "''", which represents 'path' and 'x', which are 'walls'
def read_file(path):
    with open(path, 'r') as file:
        # add lines without trailing '\n' and skip empty lines, e.g. ''' x x '' \n' -> '"''", x, x, "''"'
        lines = [l.rstrip() for l in file.readlines() if l.strip()]
        return lines


def creat_a_maze(a_list):
    """
    Create a maze which is a list of lists. Replace "''" with 0 and 'x' with 1
    e.g. a string  '' '' '' '' '' x x '' '' '' -> [0, 0, 0, 0, 0, 1, 1, 0, 0, 0]
    """
    list_of_lists = [el.split(' ') for el in a_list]
    a_maze = [[0 if el == "''" else 1 for el in l] for l in list_of_lists]
    return a_maze



def find_shortest_path(maze, x, y, path=None):

    def try_next(x, y):

        # Find a next possible position
        return [(a, b) for a, b in [(x - 1, y), (x, y - 1), (x + 1, y), (x, y + 1)] if 0 <= a < n and 0 <= b < m]

    n = len(maze)
    m = len(maze[0])

    if path is None:

        # set path to current position
        path = [(x, y)]

    # Reached destionation
    if x == DEST_X - 1 and y == DEST_Y - 1:
        return path

    # Mark current position so we won't use this cell in recursion
    maze[x][y] = 1

    # Recursively find the shortest path
    shortest_path = None
    for a, b in try_next(x, y):
        if not maze[a][b]:
            last_path = find_shortest_path(maze, a, b, path + [(a, b)])  # Solution going to a, b next

            if not shortest_path:
                shortest_path = last_path  # Use since haven't found a path yet
            elif last_path and len(last_path) < len(shortest_path):
                shortest_path = last_path  # Use since path is shorter

    maze[x][y] = 0  # Unmark so we can use this cell

    return shortest_path


if __name__ == '__main__':

    maze = creat_a_maze(read_file('maze.txt'))
    pprint(maze)

    # set a starting position
    x, y = (1, 1)
    # todo: set a destination

    shortest_path = find_shortest_path(maze, x, y)
    if shortest_path:
        pprint('Shortest path is:')
        print(shortest_path)
        pprint(f'It has length:  {len(shortest_path)}')
    else:
        pprint(f'Path not found')
