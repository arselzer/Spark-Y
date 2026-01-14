"""
Pydantic models for hypergraph representation
"""
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any


class HypergraphNode(BaseModel):
    """A node in the hypergraph (represents a relation/table)"""
    id: str
    label: str
    type: str = "relation"  # relation, attribute, aggregate
    attributes: List[str] = []
    output_attributes: List[str] = []  # Attributes that appear in SELECT clause
    cardinality: Optional[int] = None


class HypergraphEdge(BaseModel):
    """A hyperedge in the hypergraph (represents a table/relation)"""
    id: str
    nodes: List[str]  # Node IDs (equivalence classes) that this edge connects
    join_conditions: List[str] = []
    label: Optional[str] = None
    attributes: List[str] = []  # All attributes of this table (including non-join)
    output_attributes: List[str] = []  # Attributes from this table that appear in SELECT clause


class HypertreeNode(BaseModel):
    """A node in the hypertree decomposition"""
    id: str
    bag: List[str]  # Relations in this bag
    children: List[str] = []  # Child node IDs


class JoinTreeNode(BaseModel):
    """A node in the join tree (for acyclic queries)"""
    id: str
    relation: str  # The relation/table this node represents
    attributes: List[str] = []  # Attributes from this relation
    parent: Optional[str] = None  # Parent node ID in the join tree
    children: List[str] = []  # Child node IDs
    level: int = 0  # Level in the tree (0 = root)
    shared_attributes: Dict[str, List[str]] = {}  # Map of child_id -> shared attributes


class GYOStep(BaseModel):
    """A single step in the GYO reduction algorithm"""
    step_number: int
    action: str  # "remove_ear" or "remove_node"
    description: str  # Human-readable explanation
    removed_edges: List[str] = []  # Edge IDs removed in this step
    removed_nodes: List[str] = []  # Node IDs removed in this step
    remaining_edges: List[str] = []  # Edge IDs still in the hypergraph
    remaining_nodes: List[str] = []  # Node IDs still in the hypergraph

    # Phase 3: Enhanced subset relationship data
    why_ear: Optional[str] = None  # Explanation of why this is an ear (subset relationship)
    container_edges: List[str] = []  # Edges that contain the removed ear
    affected_nodes: List[str] = []  # Nodes (vertices) in the removed edge
    subset_relationships: List[Dict[str, Any]] = []  # Detailed subset info for each container


class Hypergraph(BaseModel):
    """Complete hypergraph representation of a query"""
    query_id: Optional[str] = None
    sql: str

    # Hypergraph structure
    nodes: List[HypergraphNode]
    edges: List[HypergraphEdge]

    # Hypertree decomposition (if available)
    hypertree_width: Optional[int] = None
    hypertree_decomposition: Optional[List[HypertreeNode]] = None

    # Join tree (for acyclic queries)
    join_tree: Optional[List[JoinTreeNode]] = None
    gyo_steps: Optional[List[GYOStep]] = None  # Steps of GYO reduction for visualization

    # Query properties
    is_acyclic: bool = False
    is_guarded: bool = False
    guardedness_type: str = "unknown"  # "guarded", "piecewise_guarded", "unguarded", "unknown"

    # Statistics
    num_relations: int = 0
    num_joins: int = 0
    num_aggregates: int = 0

    # Complexity Metrics
    complexity_metrics: Optional[Dict[str, Any]] = None  # Detailed complexity analysis

    # Visualization hints
    layout: Optional[Dict[str, Any]] = None
