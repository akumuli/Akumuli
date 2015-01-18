#pragma once

namespace Akumuli {

/** Put value to graphite. Graphite host should be defined in
 * `GRAPHITE_HOST` environment variable.
 */
void push_metric_to_graphite(std::string metric, double value);

}
